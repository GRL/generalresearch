import json
from datetime import datetime
from typing import Optional, List, Collection, Dict
from uuid import uuid4

from psycopg import sql

from generalresearch.grliq.models.events import (
    TimingData,
    PointerMove,
    MouseEvent,
    KeyboardEvent,
    Bounds,
)
from generalresearch.models.custom_types import UUIDStr
from generalresearch.pg_helper import PostgresConfig


class GrlIqEventManager:

    def __init__(self, postgres_config: PostgresConfig):
        self.postgres_config = postgres_config

    def update_or_create_timing(
        self,
        session_uuid: UUIDStr,
        timing_data: TimingData,
    ):
        data = {
            "session_uuid": session_uuid,
            "timing_data": (
                timing_data.model_dump_json() if timing_data is not None else None
            ),
            "uuid": uuid4().hex,
        }

        with self.postgres_config.make_connection() as conn:
            with conn.cursor() as c:
                c.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (session_uuid,))
                # Try to update first
                update_query = sql.SQL(
                    """
                    UPDATE grliq_forensicevents
                    SET timing_data = %(timing_data)s
                    WHERE session_uuid = %(session_uuid)s
                      AND timing_data IS NULL
                    RETURNING id
                """
                )
                c.execute(update_query, data)
                result = c.fetchone()

                if result:
                    pk = result["id"]
                    conn.commit()
                    return pk

                # No matching row to update. Do an insert
                insert_query = sql.SQL(
                    """
                    INSERT INTO grliq_forensicevents
                        (uuid, session_uuid, timing_data)
                    VALUES
                        (%(uuid)s, %(session_uuid)s, %(timing_data)s)
                    RETURNING id
                """
                )
                c.execute(insert_query, data)
                pk = c.fetchone()["id"]
                conn.commit()

        return pk

    def update_or_create_events(
        self,
        session_uuid: UUIDStr,
        event_start: datetime,
        event_end: datetime,
        events: Optional[List[Dict]] = None,
        mouse_events: Optional[List[Dict]] = None,
    ):
        data = {
            "uuid": uuid4().hex,
            "session_uuid": session_uuid,
            "events": json.dumps(events) if events is not None else None,
            "mouse_events": (
                json.dumps(mouse_events) if mouse_events is not None else None
            ),
            "event_start": event_start,
            "event_end": event_end,
        }

        with self.postgres_config.make_connection() as conn:
            with conn.cursor() as c:
                c.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (session_uuid,))
                # Try to update first
                update_query = sql.SQL(
                    """
                    UPDATE grliq_forensicevents
                    SET events = %(events)s, 
                        mouse_events = %(mouse_events)s,
                        event_start = %(event_start)s,
                        event_end = %(event_end)s
                    WHERE session_uuid = %(session_uuid)s
                      AND events IS NULL
                    RETURNING id
                """
                )
                c.execute(update_query, data)
                result = c.fetchone()

                if result:
                    pk = result["id"]
                    conn.commit()
                    return pk

                # No matching row to update. Do an insert
                insert_query = sql.SQL(
                    """
                     INSERT INTO grliq_forensicevents
                        (uuid, session_uuid, events, mouse_events,
                        event_start, event_end)
                     VALUES 
                        (%(uuid)s, %(session_uuid)s, %(events)s, %(mouse_events)s,
                        %(event_start)s, %(event_end)s)
                     RETURNING id
                """
                )
                c.execute(insert_query, data)
                pk = c.fetchone()["id"]
                conn.commit()

        return pk

    def filter(
        self,
        select_str: Optional[str] = None,
        session_uuid: Optional[str] = None,
        session_uuids: Optional[Collection[str]] = None,
        uuids: Optional[Collection[str]] = None,
        started_since: Optional[datetime] = None,
        limit: Optional[int] = None,
        order_by: str = "event_start DESC",
    ) -> List[Dict]:
        """ """
        if not limit:
            limit = 100
        if not select_str:
            select_str = "*"
        filters = []
        params = {}
        if session_uuid:
            params["session_uuid"] = session_uuid
            filters.append("session_uuid = %(session_uuid)s")
        if session_uuids:
            params["session_uuids"] = session_uuids
            filters.append("session_uuid = ANY(%(session_uuids)s)")
        if uuids:
            params["uuids"] = uuids
            filters.append("uuid = ANY(%(uuids)s)")
        if started_since:
            params["started_since"] = started_since
            filters.append("event_start >= %(started_since)s")

        filter_str = " AND ".join(filters)
        filter_str = "WHERE " + filter_str if filter_str else ""
        query = f"""
        SELECT {select_str}
        FROM grliq_forensicevents
        {filter_str}
        ORDER BY {order_by} LIMIT {limit}
        """
        with self.postgres_config.make_connection() as conn:
            with conn.cursor() as c:
                c.execute(query=query, params=params)
                res = c.fetchall()

        for x in res:
            if x.get("mouse_events"):
                x["mouse_events"] = [
                    PointerMove.from_dict(e) for e in x["mouse_events"]
                ]
            if x.get("timing_data"):
                x["timing_data"] = TimingData.model_validate(x["timing_data"])

            events = x.get("events", []) or []
            pointer_moves = x.get("mouse_events", []) or []
            x["mouse_events"] = self.process_mouse_events(
                events=events, pointer_moves=pointer_moves
            )
            x["keyboard_events"] = self.process_keyboard_events(events=events)
        return res

    def filter_distinct_timing(
        self,
        session_uuids: Collection[str],
    ) -> List[Dict]:
        params = {"session_uuids": list(session_uuids)}
        query = sql.SQL(
            """
        SELECT DISTINCT ON (fe.session_uuid)
            timing_data,
            fe.session_uuid,
            country_iso,
            data ->> 'client_ip_detail' as client_ip_detail
        FROM grliq_forensicevents fe
        JOIN grliq_forensicdata d on fe.session_uuid = d.session_uuid
        WHERE fe.session_uuid = ANY(%(session_uuids)s)
        AND timing_data IS NOT NULL
        ORDER BY session_uuid, fe.id DESC;
        """
        )
        with self.postgres_config.make_connection() as conn:
            with conn.cursor() as c:
                c.execute(query, params)
                res = c.fetchall()

        for x in res:
            x["timing_data"] = TimingData.model_validate(x["timing_data"])
            x["client_ip_detail"] = (
                json.loads(x["client_ip_detail"]) if x["client_ip_detail"] else None
            )

        return res

    @staticmethod
    def process_mouse_events(pointer_moves: List[PointerMove], events: List[Dict]):
        """
        In the db column 'mouse_events' we put all 'pointermove' events. Pull those
        out, and then any 'pointerdown' and 'pointerup' events from the 'events' column,
        and merge them all together into a list of MouseEvent objects
        """
        mouse_events = [
            # these contain only pointermove events
            MouseEvent(
                type=x.type,
                pageX=x.pageX,
                pageY=x.pageY,
                pointerType=x.pointerType,
                _elementId=x._elementId,
                _elementTagName=x._elementTagName,
                _elementBounds=x._elementBounds,
                timeStamp=x.timeStamp,
            )
            for x in pointer_moves
        ]
        mouse_events.extend(
            [
                MouseEvent(
                    type=x["type"],
                    pageX=x["pageX"],
                    pageY=x["pageY"],
                    pointerType=x.get("pointerType"),
                    _elementId=x.get("_elementId"),
                    _elementTagName=x.get("_elementTagName"),
                    _elementBounds=(
                        Bounds(**x["_elementBounds"])
                        if x.get("_elementBounds")
                        else None
                    ),
                    timeStamp=x["timeStamp"],
                )
                for x in events
                if x.get("type") in {"pointerdown", "pointerup", "click"}
            ]
        )
        mouse_events = sorted(mouse_events, key=lambda x: x.timeStamp)
        return mouse_events

    @staticmethod
    def process_keyboard_events(events: List[Dict]):
        res = [
            KeyboardEvent(
                type=x["type"],
                inputType=x.get("inputType"),
                key=x.get("key"),
                data=x.get("data"),
                _elementId=x.get("_elementId"),
                _elementTagName=x.get("_elementTagName"),
                timeStamp=x["timeStamp"],
                _elementBounds=(
                    Bounds(**x["_elementBounds"]) if x.get("_elementBounds") else None
                ),
            )
            for x in events
            if x.get("type") in {"keydown", "input"}
        ]
        # There's a lot of events that have nothing! on them.... ?
        res = [x for x in res if x.data or x.inputType or x.key]
        return res
