#!/usr/bin/env python3
from pathlib import Path
from typing import Tuple

from group_path.group_path_config import Config


class PathParser:

    def __init__(self, config: Config) -> None:
        self.group_assignment_year_index = config.group_assignment_year_index
        self.group_assignment_month_index = config.group_assignment_month_index
        self.group_assignment_day_index = config.group_assignment_day_index
        self.group_assignment_member_index = config.group_assignment_member_index
        self.group_assignment_max_value = max([self.group_assignment_year_index, self.group_assignment_month_index, self.group_assignment_day_index,
                                               self.group_assignment_member_index])        
        self.location_focus_source_type_index = config.location_focus_source_type_index
        self.location_focus_year_index = config.location_focus_year_index
        self.location_focus_month_index = config.location_focus_month_index
        self.location_focus_day_index = config.location_focus_day_index
        self.location_focus_location_index = config.location_focus_location_index
        if config.location_focus_path is not None:
            self.location_focus_max_value = max([self.location_focus_source_type_index, self.location_focus_year_index, self.location_focus_month_index, 
                                                 self.location_focus_day_index,self.location_focus_location_index])
        else:
            self.location_focus_max_value = None
            
        self.group_focus_year_index = config.group_focus_year_index
        self.group_focus_month_index = config.group_focus_month_index
        self.group_focus_day_index = config.group_focus_day_index
        self.group_focus_group_index = config.group_focus_group_index
        if config.group_focus_path is not None:
            self.group_focus_max_value = max([self.group_focus_year_index, self.group_focus_month_index, self.group_focus_day_index, self.group_focus_group_index])
        else:
            self.group_focus_max_value = None

    def parse_group_assignment(self, path: Path) -> Tuple[str, str, str, str, Tuple[str]]:
        parts = path.parts
        year: str = parts[self.group_assignment_year_index]
        month: str = parts[self.group_assignment_month_index]
        day: str = parts[self.group_assignment_day_index]
        member: str = parts[self.group_assignment_member_index]
        remainder: Tuple[str] = parts[self.group_assignment_max_value + 1:]
        return year, month, day, member, remainder
    
    def parse_location_focus(self, path: Path) -> Tuple[str, str, str, str, str, Tuple[str]]:
        parts = path.parts
        source_type: str = parts[self.location_focus_source_type_index]
        year: str = parts[self.location_focus_year_index]
        month: str = parts[self.location_focus_month_index]
        day: str = parts[self.location_focus_day_index]
        location: str = parts[self.location_focus_location_index]
        remainder: Tuple[str] = parts[self.location_focus_max_value + 1:]
        return source_type, year, month, day, location, remainder

    def parse_group_focus(self, path: Path) -> Tuple[str, str, str, str, Tuple[str]]:
        parts = path.parts
        year: str = parts[self.group_focus_year_index]
        month: str = parts[self.group_focus_month_index]
        day: str = parts[self.group_focus_day_index]
        group: str = parts[self.group_focus_group_index]
        remainder: Tuple[str] = parts[self.group_focus_max_value + 1:]
        return year, month, day, group, remainder
