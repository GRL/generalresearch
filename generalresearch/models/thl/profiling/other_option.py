from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from generalresearch.models.thl.profiling.upk_question import (
        UpkQuestionChoice,
    )

texts_exact = {
    "none",
    "other",
    "never",
    "na",
    "n/a",
    "nothing",
    "unsure",
    "uncertain",
    "unknown",
    "decline",
}

texts_in = {
    "none of the above",
    "none of these",
    "prefer not to answer",
    "prefer not to say",
    "dont know",
    "don't know",
    "not applicable",
    "other option",
    "other response",
    "decline to answer",
    "rather not say",
    "no answer",
    "no preference",
    "no opinion",
    "not sure",
    "i don't",
    "i dont",
    "i do not",
}


def option_is_catch_all(c: "UpkQuestionChoice") -> bool:
    """
    Exclusive not specifically in the sense that it is a multi-select question
    and if this option is selected no others can be selected. But also in the
    sense that this option should not be filtered out. It is the "catch all".
    Even a multi-select question can have >1 exclusive options.
    """
    if c.id == "-3105":
        return True
    if c.text.lower() in texts_exact:
        return True
    if any(t in c.text.lower() for t in texts_in):
        return True
    return False
