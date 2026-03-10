import shlex
from typing import Dict, List

from pydantic import BaseModel
from typing_extensions import Self

"""
e.g.: "nmap -Pn -sV -p 80,443 --reason --max-retries=3 1.2.3.4"
{'command': 'nmap',
 'options': {'p': '80,443', 'max-retries': '3'},
 'flags': ['Pn', 'sV', 'reason'],
 'positionals': ['1.2.3.4']}
"""


class ToolRunCommand(BaseModel):
    command: str
    options: Dict[str, str]
    flags: List[str]
    positionals: List[str]

    @classmethod
    def from_raw_command(cls, s: str) -> Self:
        return cls.model_validate(parse_command(s))


def parse_command(cmd: str):
    tokens = shlex.split(cmd)

    result = {
        "command": tokens[0],
        "options": {},
        "flags": [],
        "positionals": [],
    }

    i = 1
    while i < len(tokens):
        tok = tokens[i]

        # --key=value
        if tok.startswith("--") and "=" in tok:
            k, v = tok[2:].split("=", 1)
            result["options"][k] = v

        # --key value
        elif tok.startswith("--"):
            key = tok[2:]
            if i + 1 < len(tokens) and not tokens[i + 1].startswith("-"):
                result["options"][key] = tokens[i + 1]
                i += 1
            else:
                result["flags"].append(key)

        # short flag or short flag with arg
        elif tok.startswith("-"):
            if i + 1 < len(tokens) and not tokens[i + 1].startswith("-"):
                result["options"][tok[1:]] = tokens[i + 1]
                i += 1
            else:
                result["flags"].append(tok[1:])

        else:
            result["positionals"].append(tok)

        i += 1

    result["flags"] = sorted(result["flags"])
    return result
