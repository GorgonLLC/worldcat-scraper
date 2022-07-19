from datetime import datetime
import itertools
import json
import pytz
import re
import scrapy
from worldcat_scraper.databases import WorldcatScraperDatabase

class WorldcatSpider(scrapy.Spider):
    name = "worldcat"

    # these are manually taken from a search page:
    # https://www.worldcat.org/search?q=ti%3Athe&fq=ln%3Agre&dblist=638&fc=ln:_50&qt=show_more_ln%3A&cookie
    KNOWN_WORLDCAT_LANGUAGES = """
        English
        Undetermined
        Korean
        French
        German
        Spanish
        Japanese
        Arabic
        Chinese
        Russian
        Italian
        Persian
        Polish
        Portuguese
        Hebrew
        Dutch
        Latin
        Swedish
        Czech
        Multiple languages
        Ukrainian
        Thai
        Slovenian
        Afrikaans
        Danish
        Turkish
        Indonesian
        Hungarian
        Croatian
        Finnish
        Bulgarian
        Serbian
        Vietnamese
        Romanian
        Greek, Modern [1453- ]
        Lithuanian
        Catalan
        Yiddish
        Greek, Ancient [to 1453]
        Norwegian
        Sanskrit
        Slovak
        Tibetan
        Welsh
        Ndonga
        Malay
        Hindi
        Irish
        Miscellaneous languages
        Bosnian
        English, Middle [1100-1500]
        Urdu
        Tamil
        Armenian
        Estonian
        Belarusian
        Bokmal, Norwegian
        Bengali
        Scots
        Maori
        Macedonian
        Tagalog
        Nauru
        Burmese
        Latvian
        Scottish Gaelic
        Mongolian
        Icelandic
        English, Old [ca. 450-1100]
        Sinhalese
        French, Old [ca. 842-1300]
        Ladino
        Church Slavic
        Georgian
        Kurdish
        Syriac, Modern
        Azerbaijani
        French, Middle [ca. 1300-1600]
        Albanian
        Pali
        Turkish, Ottoman
        Gujarati
        Romance [Other]
        Basque
        Austronesian [Other]
        Khmer
        Panjabi
        Telugu
        Zulu
        North American Indian [Other]
        Marathi
        Niger-Kordofanian [Other]
        Hawaiian
        Papuan [Other]
        Swahili
        Amharic
        Galician
        Aramaic
        Bantu [Other]
        Akkadian
        """
    KNOWN_WORLDCAT_LANGUAGES_SET = set([y for y in (x.strip() for x in KNOWN_WORLDCAT_LANGUAGES.splitlines()) if y])

    def __init__(self, start_id=1, end_id=None, exclude_saved='t', exclude_ranges='[]', **kwargs):
        self.start_id = int(start_id)
        self.end_id = int(end_id) if end_id else None
        self.exclude_saved = exclude_saved.lower() in ['true', 't', 'yes', 'y']
        self.exclude_ranges = json.loads(exclude_ranges)
        self.database = WorldcatScraperDatabase()
        super().__init__(**kwargs)

    def start_requests(self):
        iterator = None
        if self.end_id:
            iterator = range(self.start_id, self.end_id)
        else:
            iterator = itertools.count(self.start_id)
        for i in iterator:
            # skip over IDs within provided exclude ranges
            skip = False
            for start, end in self.exclude_ranges:
                if start <= i <= end:
                    skip = True
                    break
            if skip:
                continue
            # skip over IDs that already exist in the database
            if self.exclude_saved:
                for row in self.database.dbExecute("SELECT 1 FROM books WHERE oclc_id = ?", (i,)):
                    skip = True
                if skip:
                    continue
            url = 'https://www.worldcat.org/oclc/{}'.format(i)
            request = scrapy.Request(
                url=url,
                callback=self.parse,
                cb_kwargs=dict(oclc_id=i))
            yield request

    def parse(self, response, oclc_id):
        result = {
            'oclc_id': oclc_id,
            'updated_at': datetime.now(tz=pytz.UTC).isoformat(),
            'data': {},
        }
        # WorldCat returns 200 OK for books that are not found :/
        if response.xpath("//*[@id='div-maincol']/p[contains(.,'page you tried was not found')]"):
            result['status'] = 0 # 'not_found'
            return result
        else:
            result['status'] = 1 # 'found'

        for row in response.xpath('//*[@id="details"]/div/table/tr'):
            key = row.xpath('th/text()').get()
            match key:
                case 'Genre/Form:':
                    key = 'genre'
                case 'Additional Physical Format:':
                    key = 'addtl_physical_format'
                case 'Named Person:':
                    key = 'named_person'
                case 'Material Type:':
                    key = 'material_type'
                case 'Document Type:':
                    key = 'doc_type'
                case 'All Authors / Contributors:':
                    key = 'authors'
                case 'ISSN:':
                    key = 'issn'
                case 'ISBN:':
                    key = 'isbn'
                case 'OCLC Number:':
                    key = 'oclc'
                case 'Language Note:':
                    key = 'language_note'
                case 'Notes:':
                    key = 'notes'
                case 'Performer(s):':
                    key = 'performers'
                case 'Credits:':
                    key = 'credits'
                case 'Description:':
                    key = 'description'
                case 'Contents:':
                    key = 'contents'
                case 'Other Titles:':
                    key = 'other_titles'
                case 'Awards:':
                    key = 'awards'
                case 'Responsibility:':
                    key = 'responsibility'
                case 'Series Title:':
                    key = 'series_title'
                case 'More information:':
                    key = 'more_information'
                case x:
                    self.crawler.engine.close_spider(self, reason='UNKNOWN KEY {} encountered on oclc_id={}'.format(x, oclc_id))
                    return None

            match key:
                case 'authors':
                    # multi-element, inside nested <a>s
                    value = row.xpath('td/a/text()').getall()
                case 'isbn' | 'issn':
                    # multi-element, separated by spaces
                    value = row.xpath('td/text()').get().split()
                case 'named_person':
                    # multi-element, separated by semicolons
                    value = row.xpath('td/text()').get().split('; ')
                    # also needs to be de-duped (e.g. OCLC #6)
                    value = list(set(value))
                case 'genre' | 'addtl_physical_format' | 'notes':
                    # multi-element, separated by <br>s
                    value = row.xpath('td/text()').getall()
                case 'series_title':
                    # single element, stored in one nested <a>
                    value = row.xpath('td/a/text()').get()
                case 'more_information':
                    # multi-element, stored in <a>s inside a <ul>
                    value = row.css('.inlinelinks a').getall()
                case _:
                    # everything else is single-element
                    value = row.xpath('td/text()').get()

            result['data'][key] = value

        # top-level data:
        result['data']['title'] = response.css('#bibdata > h1::text').get()
        if cover := response.css('#cover > img::attr(src)').get():
            result['data']['cover'] = re.sub('^//', 'https://', cover)
        result['data']['publisher'] = response.css('#bib-publisher-cell::text').get()
        result['data']['edition_format'] = response.css('#editionFormatType .itemType::text').get()
        #                                          response.xpath('//*[@id="editionFormatType"]/descendant-or-self::*/text()').getall()
        #                                          response.css('#editionFormatType *::text').getall()
        editions_ = [x.strip().split(':') for x in response.xpath('//*[@id="editionFormatType"]/text()').getall()]
        editions = [x.strip() for x in list(itertools.chain(*editions_)) if x != '']
        # Language is listed in "Edition/Format" section, *usually* as the last element,
        # but not always (e.g. OCLC #17, #25), and there is no other tagging of this data element,
        # so we simply check membership of each "edition" element in the list of known languages
        result['data']['language'] = None
        for x in editions:
            if x in self.KNOWN_WORLDCAT_LANGUAGES_SET:
                result['data']['language'] = x
                break
        if result['data']['language'] is None:
            self.crawler.engine.close_spider(self, reason='UNKNOWN LANGUAGE encountered on oclc_id={}'.format(oclc_id))
            return None
        result['data']['external_links'] = []
        for x in response.css('#ecopy p'):
            link = {
                "title": x.css("a::attr(title)").get(),
                "href": x.css("a::attr(href)").get(),
                "text": x.css("span::text").get(),
            }
            result['data']['external_links'].append(link)
        if abstract := response.xpath('//*[@id="details"]/div/div/div/text()').get():
            extra = response.css('#details > div > div > div > span.showMoreLessContentElement::text').get()
            if extra:
                result['data']['abstract'] = abstract.lstrip() + extra
            else:
                result['data']['abstract'] = abstract.lstrip()
        else:
            result['data']['abstract'] = None
        result['data']['related_subjects'] = response.css('#subject-terms-detailed > li > a::text').getall()

        return result
