import inflect
import numpy as np
import pandas as pd
import unicodedata

from fractions import Fraction

ie = inflect.engine()
ie.defnoun('craft', 'crafts')

def to_singular(word: str):
    return ie.singular_noun(word) if ie.singular_noun(word) else word

def to_plural(word: str):
    if ie.singular_noun(word): 
        return ie.plural(ie.singular_noun(word))
    
    return ie.plural(word)

# used by pattern_needle_sizes
def fraction_to_float(fraction: str):
    if pd.isna(fraction):
        return np.nan
    if isinstance(fraction, (int, float)):
        return float(fraction)

    fraction = str(fraction).strip()
    if not fraction:
        return np.nan

    try:
        return float(fraction)
    except ValueError:
        pass

    try:
        frac_val = unicodedata.numeric(fraction[-1])
        whole_num = fraction[:-1].strip()
        return float(whole_num) + frac_val if whole_num else float(frac_val)
    except (TypeError, ValueError):
        pass

    try:
        parts = fraction.split()
        if len(parts) == 2:
            whole_num, frac = parts
            return float(whole_num) + float(Fraction(frac))
    except (ValueError, ZeroDivisionError):
        return np.nan
