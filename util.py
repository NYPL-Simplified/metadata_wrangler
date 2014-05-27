"Miscellaneous utilities"
import pkgutil
import collections

class LanguageCodes(object):
    """Convert between ISO-639-2 and ISO-693-1 language codes.

    The data file comes from
    http://www.loc.gov/standards/iso639-2/ISO-639-2_utf-8.txt
    """

    two_to_three = collections.defaultdict(lambda: None)
    three_to_two = collections.defaultdict(lambda: None)
    english_names = collections.defaultdict(list)

    data = pkgutil.get_data(
        "resources", "ISO-639-2_utf-8.txt")

    for i in data.split("\n"):
        (alpha_3, terminologic_code, alpha_2, names,
         french_names) = i.strip().split("|")
        names = [x.strip() for x in names.split(";")]
        if alpha_2:
            three_to_two[alpha_3] = alpha_2
            english_names[alpha_2] = names
            two_to_three[alpha_2] = alpha_3
        english_names[alpha_3] = names
