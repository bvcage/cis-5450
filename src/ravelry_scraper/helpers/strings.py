import inflect
import unicodedata
from fractions import Fraction

ie = inflect.engine()
ie.defnoun('craft', 'crafts')

def to_singular(word: str):
    return ie.singular_noun(word) if ie.singular_noun(word) else word

def to_plural(word: str):
    return ie.plural(word) if not ie.singular_noun(word) else word


### For pattern_needle_sizes
def fraction_to_float(fraction: str):
    try:
        frac_val = unicodedata.numeric(fraction[-1])
        whole_num = fraction[:-1].strip()

        if whole_num:
            return float(whole_num) + frac_val
        else:
            return frac_val
    except (TypeError, ValueError):
        return fraction
