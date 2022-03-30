#!/usr/bin/env python3
import math


# get bit value from a base10 number
def get_nth_bit(number, n) -> int:
    return (int(number) >> n) & 0x0001


# reverse bit value
def get_nth_bit_opposite(number, n) -> int:
    bit = get_nth_bit(number, n)
    if bit == 1:
        return 0
    else:
        return 1


# get base10 value from base2 bit range start to end
def get_range_bits(value, start, end) -> int:
    mask = ~(-1 << (end - start + 1)) << start
    return (int(value) & mask) >> start


def get_temp_kelvin(temp: float) -> float:
    return temp + 273.15


def get_pressure_pa(pres: float) -> float:
    return pres * 1000


def mmol_to_mol(mole: float) -> float:
    return mole / 1000


def umol_to_mol(mole: float) -> float:
    return mole / 1000000


def from_percentage(percent: float) -> float:
    return percent / 100


def get_degree_radian(degree: float) -> float:
    return degree * math.pi / 180
