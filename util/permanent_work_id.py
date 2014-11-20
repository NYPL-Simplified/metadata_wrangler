import md5
import re
import struct

class Calculator(object):

    @classmethod
    def permanent_id(self, normalized_title, normalized_author, 
                     grouping_category):
        digest = md5.new()
        for i in (normalized_title, normalized_author, grouping_category):
            if i == '' or i is None:
                i = '--null--'
            digest.update(i.encode("ascii"))
        permanent_id = digest.hexdigest().zfill(32)
        permanent_id = "-".join([
            permanent_id[:8], permanent_id[8:12], permanent_id[12:16],
            permanent_id[16:20], permanent_id[20:]])
        return permanent_id

    # Strings to be removed from author names.
    authorExtract1 = re.compile("^(.+?)\\spresents.*$");
    authorExtract2 = re.compile("^(?:(?:a|an)\\s)?(.+?)\\spresentation.*$")
    distributedByRemoval = re.compile("^distributed (?:in.*\\s)?by\\s(.+)$")
    initialsFix = re.compile("(?<=[A-Z])\\.(?=(\\s|[A-Z]|$))")
    apostropheStrip = re.compile("'s")
    specialCharacterStrip = re.compile("[^\\w\\s]")
    consecutiveCharacterStrip = re.compile("\\s{2,}")
    bracketedCharacterStrip = re.compile("\\[(.*?)\\]")
    commonAuthorSuffixPattern = re.compile("^(.+?)\\s(?:general editor|editor|editor in chief|etc|inc|inc\\setc|co|corporation|llc|partners|company|home entertainment)$")
    commonAuthorPrefixPattern = re.compile("^(?:edited by|by the editors of|by|chosen by|translated by|prepared by|translated and edited by|completely rev by|pictures by|selected and adapted by|with a foreword by|with a new foreword by|introd by|introduction by|intro by|retold by)\\s(.+)$")

    format_to_grouping_category = {
        "Atlas": "other",
        "Map": "other",
        "TapeCartridge": "other",
        "ChipCartridge": "other",
        "DiscCartridge": "other",
        "TapeCassette": "other",
        "TapeReel": "other",
        "FloppyDisk": "other",
        "CDROM": "other",
        "Software": "other",
        "Globe": "other",
        "Braille": "book",
        "Filmstrip": "movie",
        "Transparency": "other",
        "Slide": "other",
        "Microfilm": "other",
        "Collage": "other",
        "Drawing": "other",
        "Painting": "other",
        "Print": "other",
        "Photonegative": "other",
        "FlashCard": "other",
        "Chart": "other",
        "Photo": "other",
        "MotionPicture": "movie",
        "Kit": "other",
        "MusicalScore": "book",
        "SensorImage": "other",
        "SoundDisc": "audio",
        "SoundCassette": "audio",
        "SoundRecording": "audio",
        "VideoCartridge": "movie",
        "VideoDisc": "movie",
        "VideoCassette": "movie",
        "VideoReel": "movie",
        "Video": "movie",
        "MusicalScore": "book",
        "MusicRecording": "music",
        "Electronic": "other",
        "PhysicalObject": "other",
        "Manuscript": "book",
        "eBook": "ebook",
        "Book": "book",
        "Newspaper": "book",
        "Journal": "book",
        "Serial": "book",
        "Unknown": "other",
        "Playaway": "audio",
        "LargePrint": "book",
        "Blu-ray": "movie",
        "DVD": "movie",
        "VerticalFile": "other",
        "CompactDisc": "audio",
        "TapeRecording": "audio",
        "Phonograph": "audio",
        "pdf": "ebook",
        "epub": "ebook",
        "jpg": "other",
        "gif": "other",
        "mp3": "audio",
        "plucker": "ebook",
        "kindle": "ebook",
        "externalLink": "ebook",
        "externalMP3": "audio",
        "interactiveBook": "ebook",
        "overdrive": "ebook",
        "external_web": "ebook",
        "external_ebook": "ebook",
        "external_eaudio": "audio",
        "external_emusic": "music",
        "external_evideo": "movie",
        "text": "ebook",
        "gifs": "other",
        "itunes": "audio",
        "Adobe_EPUB_eBook": "ebook",
        "Kindle_Book": "ebook",
        "Microsoft_eBook": "ebook",
        "OverDrive_WMA_Audiobook": "audio",
        "OverDrive_MP3_Audiobook": "audio",
        "OverDrive_Music": "music",
        "OverDrive_Video": "movie",
        "OverDrive_Read": "ebook",
        "Adobe_PDF_eBook": "ebook",
        "Palm": "ebook",
        "Mobipocket_eBook": "ebook",
        "Disney_Online_Book": "ebook",
        "Open_PDF_eBook": "ebook",
        "Open_EPUB_eBook": "ebook",
        "eContent": "ebook",
        "SeedPacket": "other",
    }

    @classmethod
    def normalize_author(cls, author):
        groupingAuthor = cls.initialsFix.sub(" ", author)
        groupingAuthor = cls.bracketedCharacterStrip.sub("", groupingAuthor)
        groupingAuthor = cls.specialCharacterStrip.sub(
            " ", groupingAuthor).strip().lower();
        groupingAuthor = cls.consecutiveCharacterStrip.sub(" ", groupingAuthor)

        # extract common additional info (especially for movie studios)
        # Remove home entertainment
        for regexp in [
                cls.authorExtract1, cls.authorExtract2, 
                cls.commonAuthorSuffixPattern,
                cls.commonAuthorPrefixPattern, cls.distributedByRemoval
                ]:
            match = regexp.search(groupingAuthor)
            if match:
                groupingAuthor = match.groups()[0]

        # Remove md if the author ends with md
        if groupingAuthor.endswith(" md"):
            groupingAuthor = groupingAuthor[:-3]

        if len(groupingAuthor) > 50:
            groupingAuthor = groupingAuthor[:50]
        groupingAuthor = groupingAuthor.strip()

        # TODO: I don't understand this yet.
        # groupingAuthor = RecordGroupingProcessor.mapAuthorAuthority(groupingAuthor);
        return groupingAuthor


import csv

for row_number, solution, title, author, media, timestamp in csv.reader(open("grouped_work_sample_large.csv")):
    if solution != Calculator.permanent_id(title, author, media):
        print title, repr(author), media

