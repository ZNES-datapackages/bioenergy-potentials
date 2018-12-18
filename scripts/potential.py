"""
"""
import os
import re

import pandas as pd

from shutil import copy
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.converter import TextConverter
from pdfminer.layout import LAParams
from pdfminer.pdfpage import PDFPage
from io import StringIO
from datapackage import Package
from datapackage_utilities import building


def frame_from_text(txt, searchfor):
    """ Create dataframe from singular page content.

    searchfor : list
        List of countries to look for as index.
    """

    scenarios = [
        'low availability scenario', 'medium availability scenario',
        'high availability scenario']
    years = range(2010, 2060, 10)  # years

    columns = pd.MultiIndex.from_product([scenarios, years])

    txt = txt.split('\n\n')

    index = list(filter(lambda i: i.strip() in searchfor, txt))

    start = -next(i for i, item in enumerate(reversed(txt)) if item == '2050 ')
    end = next(i for i, item in enumerate(txt) if '| P a g e' in item)
    data = txt[start:end]

    # indices get sometimes mixed up in the data and have to be filtered
    data = list(filter(lambda i: i.strip() not in searchfor, data))

    # https://stackoverflow.com/questions/2233204/how-does-zipitersn-work-in-python
    data = list(zip(*[iter(data)]*15))  # 15 columns

    return pd.DataFrame(data, index=index, columns=columns)


# difficult to read tables
copy('archive/Table_31_Grassy_crops_biomass_potential.csv', 'data/')
copy('archive/Table_32_Willow_biomass_potential.csv', 'data/')
copy('archive/Table_33_Poplar_biomass_potential.csv', 'data/')
ignore_pages = [103, 105, 107]

countries = pd.DataFrame(
    Package(
        'https://raw.githubusercontent.com/datasets/'
        'country-codes/master/datapackage.json')
    .get_resource('country-codes').read(keyed=True)) \
    .set_index('ISO3166-1-Alpha-2', drop=False)['ISO3166-1-Alpha-2']

countries['GB'] = 'UK'
countries = list(countries.dropna())

path = building.download_data('https://setis.ec.europa.eu/sites/default/files/reports/biomass_potentials_in_europe.pdf')

# https://stackoverflow.com/questions/26494211/extracting-text-from-a-pdf-file-using-pdfminer-in-python
with open(path, 'rb') as fp:
    for p in range(95, 129)[::2]:  # first pages
        if p not in ignore_pages:

            rsrcmgr = PDFResourceManager()
            codec = 'utf-8'
            laparams = LAParams()

            retstr = StringIO()
            device = TextConverter(
                rsrcmgr, retstr, codec=codec, laparams=laparams)
            interpreter = PDFPageInterpreter(rsrcmgr, device)

            p1, p2 = PDFPage.get_pages(
                fp, pagenos=[p + 1, p + 2], check_extractable=True)

            interpreter.process_page(p1)
            txt = retstr.getvalue()

            name = next(i for i in txt.split('\n\n') if 'Table' in i)
            name = re.sub('[^a-zA-Z0-9_]', '', '_'.join(name.split()[:-1]))

            df = frame_from_text(txt, countries)

            # https://stackoverflow.com/questions/4330812/how-do-i-clear-a-stringio-object
            retstr.truncate(0)
            retstr.seek(0)

            interpreter.process_page(p2)
            df = df.append(frame_from_text(retstr.getvalue(), countries))

            device.close()

            df = df.stack(level=1)
            df.replace('- ', 'NaN', inplace=True)
            df.index.names = ['country', 'year']
            df.to_csv(os.path.join('data', name + '.csv'))

building.metadata_from_data(directory='data')
