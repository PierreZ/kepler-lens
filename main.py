import os
import wget
import tarfile
import click
import kplr
from string import Template
from pyke import kepconvert
import json
import sys

ARCHIVE_URL = 'https://archive.stsci.edu/pub/{}/lightcurves/tarfiles/'

K2_TARFILES = {
    'c1': 2,
    'c102': 2,
    'c111': 1,
    'c112': 2,
    'c12': 3,
    'c3': 3,
    'c4': 1,
    'c5': 2,
    'c6': 3,
    'c7': 1,
    'c8': 2
}

KEPLER_TARFILES = {
    'Q0': 1,
    'Q1': 6,
    'Q2': 10,
    'Q3': 10,
    'Q4': 10,
    'Q5': 10,
    'Q6': 10,
    'Q7': 10,
    'Q8': 8,
    'Q9': 11,
    'Q10': 10,
    'Q11': 10,
    'Q12': 9,
    'Q13': 10,
    'Q14': 11,
    'Q15': 10,
    'Q16': 10,
    'Q17': 4
}

META_UPDATE_MC2_TEMPLATE = """
<'
$attributes
'>
JSON-> 'data' STORE 

MARK

'$rt' '~.*' { 'filename' '$filenames' } NOW NOW ] FETCH

<%
    DUP NAME RENAME
    $$data 'attributes' GET SETATTRIBUTES
%> FOREACH
COUNTTOMARK ->LIST '$wt' META
"""


@click.group()
def cli():
    pass

@cli.command()
@click.option('--path', default='/tmp/kepler', help='kepler download folder')
@click.option('--limit', default='all', help='comma separated list of compagne to download.')
@click.argument('dataset', type=click.Choice(['k2', 'kepler']))
def init(path, limit, dataset):
    if dataset == "kepler":
        download_campagne(path, limit, "kepler", KEPLER_TARFILES)
    if dataset == "k2":
        download_campagne(path, limit, "k2", K2_TARFILES)


def download_campagne(path, limit, dataset, dictFiles):
    click.echo('Initializing the database, downloading {} dataset...'.format(dataset))

    baseurl = ARCHIVE_URL.format(dataset)

    lightcurvesfolder = path + "/lightcurves/"
    csvfolder = path + "/csv/"

    if not os.path.exists(path):

        click.echo('Creating folder {}'.format(lightcurvesfolder))
        os.makedirs(lightcurvesfolder)
        click.echo('Creating folder {}'.format(csvfolder))
        os.makedirs(csvfolder)

    if limit != "all":
        limits = limit.split(",")
        click.echo('Filtering compagne, only using {}'.format(limit))
        compagnes = {k:v for (k, v) in dictFiles.items() if k in limits}
    else:
        compagnes = dictFiles

    for compagne, nbrfiles in compagnes.items():
        click.echo('downloading {} dataset, {} files'.format(compagne, nbrfiles))
        for nbrfile in range(1, nbrfiles + 1):
            dl_campagne(compagne, nbrfile, lightcurvesfolder, baseurl, dataset)
            generate_csv(compagne, nbrfile, lightcurvesfolder, csvfolder)

        click.echo('compagne {} done!'.format(compagne))

    click.echo('all compagnes are fetched, bye')

def generate_csv(compagne, nbrfile, lightcurvesfolder, csvfolder):

    filename = 'public_{}_long_{}/'.format(compagne, nbrfile)
    lightcurvesfolder = lightcurvesfolder + filename
    directory = os.fsencode(lightcurvesfolder)

    for file in os.listdir(directory):
        filename = os.fsdecode(file)
        if filename.endswith(".fits") and not os.path.exists(csvfolder + filename.replace(".fits", ".csv")):
            # Generating CSV 
            kepconvert(lightcurvesfolder + filename, "fits2csv", "TIME,SAP_FLUX,SAP_FLUX_ERR,SAP_QUALITY",
                       outfile=csvfolder + filename.replace(".fits", ".csv"),
                       baddata=False, overwrite=False, verbose=False)
            # print(os.path.join(directory, filename))
            continue
        else:
            continue


def dl_campagne(compagne, nbrfile, lightcurvesfolder, baseurl, dataset):

    filename = 'public_{}_long_{}'.format(compagne, nbrfile)
    outfolder = lightcurvesfolder + filename
    # checking if already downloaded
    if os.path.exists(outfolder):
        click.echo('folder {} already exists'.format(outfolder))
        click.echo('{} already downloaded, moving on'.format(filename))
        return

    os.makedirs(outfolder)
    url = baseurl + compagne
    if dataset == "kepler":
        url = url + "_public"
    url = url + "/" + filename + ".tgz"

    click.echo('downloading {}'.format(url))
    outfile = lightcurvesfolder+filename+".tgz"
    wget.download(url, out=outfile)

    click.echo('{} downloaded, untar in progress'.format(filename))

    tar = tarfile.open(outfile)
    tar.extractall(path=outfolder)
    tar.close()

    click.echo('untar of {} done'.format(filename))
    os.remove(outfile)
    click.echo('removing archive {} done'.format(outfile))


@cli.command()
@click.option('--wtoken', default='w', help='warp WRITE token')
@click.option('--rtoken', default='r', help='warp READ token')
@click.option('--endpoint', default='http://localhost:8080', help='warp endpoint')
@click.option('--limit', default=0, help='limit on koi_period')
def update(wtoken, limit):

    client = kplr.API()

    click.echo('Fetching all KOI where koi_period>{}'.format(limit))
    # Getting all Kepler object of Interests, not K2
    kois = client.kois(where="koi_period>{}".format(limit), sort="kepid")

    click.echo('Found {} KOIS'.format(len(kois)))

    koisdict = {}

    for koi in kois:
        disposition = koi.koi_disposition
        score = koi.koi_score
        kepid = koi.kepid
        kepoi_name = koi.kepoi_name
        kepler_name = koi.kepler_name

        if score is None:
            score = -1

        if kepler_name is None:
            kepler_name = ''

        # Example:
        # kepoi_name:K00992.01, kepler_name:Kepler-745 b, disposition:CONFIRMED, score:0
        click.echo('fetched info for {}: kepoi_name:{}, kepler_name:{}, disposition:{}, score:{}'
                   .format(kepid, kepoi_name, kepler_name, disposition, score))

        lcs = koi.get_light_curves(short_cadence=False)

        files = []

        for lcsfile in lcs:
            _, filename = os.path.split(lcsfile.filename)
            files.append(filename)

            if "":
                click.echo("ktwo found! {}".format(filename))
                sys.exit(1)

        if  kepid in koisdict:
            koisdict[kepid]['attributes']['{}'.format(kepoi_name)] = {
                'kepler_name': kepler_name,
                'disposition': disposition,
                'score': score,
            }
            koisdict[kepid]['attributes']['size'] += 1
        else:
            koisdict[kepid] = {
                'filenames': files,
                'kepid': kepid,
                'attributes': {
                    '{}'.format(kepoi_name): {
                        'kepler_name': kepler_name,
                        'disposition': disposition,
                        'score': score,
                    },
                    'size': 1,
                },
            }

    click.echo("attributes fetched, updating GTS with the new attributes")

    for _, value in koisdict.items():
        mc2 = Template(META_UPDATE_MC2_TEMPLATE)
        mc2 = mc2.substitute(rt=rtoken, wt=wtoken, 
                             filenames="~(" + ")|(".join(value['filenames']) + ")",
                             attributes=json.dumps(value['attributes']))
        click.echo('{}'.format(mc2))

if __name__ == '__main__':
    cli()