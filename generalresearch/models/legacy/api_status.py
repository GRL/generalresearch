from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel, Field

"""
    Messed up consistency, and we have multiple different formats 
    for error reporting for no reason. Faithfully recreating them here...
"""


class StatusResponse(BaseModel):
    status: Literal["success", "error"] = Field(
        description="The status of the API response.", examples=["success"]
    )
    msg: Optional[str] = Field(
        description="An optional message, if the status is error.",
        examples=[""],
        default=None,
    )


class StatusResponseError(BaseModel):
    status: Literal["error"] = Field(
        description="The status of the API response.", examples=["error"]
    )
    msg: str = Field(
        description="An optional message, if the status is error.",
        examples=["An error has occurred"],
    )


class StatusResponseFailure(BaseModel):
    status: Literal["failure"] = Field(
        description="The status of the API response.", examples=["failure"]
    )
    msg: str = Field(
        description="An optional message, if the status is failure.",
        examples=["An error has occurred"],
    )


class StatusSuccess(BaseModel):
    success: bool = Field(
        default=True, description="Whether the API response is successful."
    )


class StatusSuccessFail(StatusSuccess):
    success: bool = Field(
        default=False, description="Whether the API response is successful."
    )


class StatusInfoResponse(BaseModel):
    info: StatusSuccess = Field()
    msg: str = Field(
        description="An optional message, if success is False",
        examples=[""],
        default="",
    )


class StatusInfoResponseFail(BaseModel):
    info: StatusSuccessFail = Field()
    msg: str = Field(
        description="An optional message, if success is False",
        examples=["An error has occurred"],
    )
