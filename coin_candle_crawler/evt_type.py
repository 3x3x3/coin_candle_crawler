# -*- coding: utf-8 -*-
from enum import Enum


class EvtType(Enum):
    INSERT_TO_DB = 0
    RCV_FINISHED = 1
