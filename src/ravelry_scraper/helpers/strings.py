import inflect

ie = inflect.engine()
ie.defnoun('craft', 'crafts')

def to_singular(word: str):
    return ie.singular_noun(word) if ie.singular_noun(word) else word

def to_plural(word: str):
    return ie.plural(word) if not ie.singular_noun(word) else word