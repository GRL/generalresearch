import html
import webbrowser
from typing import List

import numpy as np
from more_itertools import windowed
from scipy.spatial.distance import euclidean

from generalresearch.grliq.managers.colormap import turbo_colormap_data
from generalresearch.grliq.models.events import KeyboardEvent, MouseEvent


def make_events_svg(
    mouse_events: List[MouseEvent], keyboard_events: List[KeyboardEvent]
) -> str:
    if len(mouse_events) + len(keyboard_events) == 0:
        return f'<svg xmlns="http://www.w3.org/2000/svg">\n' + "\n</svg>"

    t = np.array([pm.timeStamp for pm in mouse_events])
    t_diff = t.max() - t.min()
    for x in mouse_events:
        if x.type in {"pointerdown", "pointerup"} and x.pointerType == "touch":
            x.type = "pointermove"
    move_events = [x for x in mouse_events if x.type == "pointermove"]
    clicks = [x for x in mouse_events if x.type == "click"]
    click_type = (
        "touch" if any(x.pointerType == "touch" for x in mouse_events) else "mouse"
    )

    svg_elements = []
    for ee in windowed(move_events, 2):
        e1 = ee[0]
        e2 = ee[1]

        assert e1 is not None
        assert e2 is not None

        ts_idx = (e2.timeStamp - t.min()) / t_diff
        r, g, b = turbo_colormap_data[round(ts_idx * 255)]
        color = f"rgb({int(r*255)},{int(g*255)},{int(b*255)})"
        svg_elements.append(
            f'<line x1="{e1.pageX}" y1="{e1.pageY}" x2="{e2.pageX}" y2="{e2.pageY}" '
            f'stroke="{color}" stroke-width="4" />'
        )
    for c in clicks:
        cx = c.pageX
        cy = c.pageY
        if cx is not None and cy is not None:
            ts_idx = (c.timeStamp - t.min()) / t_diff
            r, g, b = turbo_colormap_data[round(ts_idx * 255)]
            color = f"rgb({int(r*255)},{int(g*255)},{int(b*255)})"
            if c._elementBounds is not None:
                b = c._elementBounds
                svg_elements.append(
                    f'<rect x="{b.left}" y="{b.top}" width="{b.width}" height="{b.height}" '
                    f'fill="none" stroke="blue" stroke-width="2" />'
                )
            if click_type == "mouse":
                svg_elements.append(
                    f'<circle cx="{cx}" cy="{cy}" r="6" fill="red" stroke="black" stroke-width="1" />'
                )
            else:
                # Inner solid red circle
                svg_elements.append(
                    f'<circle cx="{cx}" cy="{cy}" r="6" fill="{color}" stroke="black" stroke-width="1" />'
                )
                # Middle semi-transparent larger circle
                svg_elements.append(
                    f'<circle cx="{cx}" cy="{cy}" r="12" fill="{color}" fill-opacity="0.3" />'
                )
                # Outer faint larger circle with more transparency
                svg_elements.append(
                    f'<circle cx="{cx}" cy="{cy}" r="18" fill="{color}" fill-opacity="0.15" />'
                )

    groups = group_input_events_by_xy(
        mouse_events=mouse_events, keyboard_events=keyboard_events
    )

    for group in groups:
        cx, cy = group[0]
        text = "".join(group[1])
        text = text.replace("DELETECONTENTBACKWARD", "BACKSPACE")
        text = text.replace(">", ">\n")
        if len(text) > 5:
            font_size = 10
        else:
            font_size = 20
        svg_elements.append(svg_multiline_text(text, cx + 5, cy - 5, font_size))

    svg = (
        f'<svg xmlns="http://www.w3.org/2000/svg">'
        + "\n".join(svg_elements)
        + "\n</svg>"
    )
    return svg


def view_plot(svg: str):
    fp = "/tmp/test.svg"
    with open(fp, "w") as f:
        f.write(svg)
    webbrowser.open("file://" + fp)


def svg_multiline_text(
    text: str, x: float, y: float, font_size: int = 20, line_spacing: float = 1.2
) -> str:
    lines = html.escape(text).split("\n")
    tspan_elements = [f'<tspan x="{x}" dy="0">{lines[0]}</tspan>'] + [
        f'<tspan x="{x}" dy="{font_size * line_spacing}">{line}</tspan>'
        for line in lines[1:]
    ]
    return (
        f'<text x="{x}" y="{y}" font-family="Arial" font-size="{font_size}" '
        f'fill="red" stroke="white" stroke-width="4" paint-order="stroke">'
        f"{''.join(tspan_elements)}</text>"
    )


def group_input_events_by_xy(
    mouse_events: List[MouseEvent], keyboard_events: List[KeyboardEvent]
) -> List[tuple[tuple[float, float], List[str]]]:
    """
    Each keypress is its own event. For plotting, we want to group together
    all keypresses that were made when the mouse was at the same position,
    and then concat them. Otherwise, if we just plot each letter at the position
    where the mouse was, then if the mouse doesn't move, all letter will be on top
    of each other.
    """
    groups = []
    last_pos = None
    last_time = None
    current_chars = []
    for e in keyboard_events:
        # Get the most recent position of the mouse in the time before the key was pressed
        mouse_events_before = [x for x in mouse_events if x.timeStamp < e.timeStamp]
        if mouse_events_before:
            mouse_event = mouse_events_before[-1]
            cx = mouse_event.pageX
            cy = mouse_event.pageY
        else:
            cx, cy = 0, 0
        char = e.text
        if not char:
            continue
        if last_time is None:
            last_time = e.timeStamp
        if last_pos is None:
            last_pos = (cx, cy)
            current_chars.append(char)
        else:
            if abs(last_time - e.timeStamp) < 2000:
                char = char + "\n"
            if euclidean((cx, cy), last_pos) < 20:
                current_chars.append(char)
            else:
                groups.append((last_pos, current_chars))
                current_chars = [char]
                last_pos = (cx, cy)
    if current_chars:
        groups.append((last_pos, current_chars))
    return groups
