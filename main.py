import os
from os.path import expanduser
import wget
import tarfile
import click


K2_URL = 'https://archive.stsci.edu/pub/k2/lightcurves/tarfiles/'
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


@click.group()
def cli():
    pass

@cli.command()
@click.option('--token', default='', help='warp WRITE token')
@click.option('--path', default='/tmp/kepler', help='kepler download folder')
@click.option('--limit', default='all', help='comma separated list of compagne to download.')
def init(token, path, limit):
    click.echo('Initializing the database')
    click.echo('Downloading K2 compagnes...')

    lightcurvesfolder = path + "/lightcurves/"
    csvfolder = path + "/csv/"

    if not os.path.exists(path):

        click.echo('Creating folder {}'.format(lightcurvesfolder))
        os.makedirs(lightcurvesfolder)
        click.echo('Creating folder {}'.format(csvfolder))
        os.makedirs(csvfolder)

    if limit is not "all":
        limits = limit.split(",")
        click.echo('Filtering compagne, only using {}'.format(limit))
        compagnes = {k:v for (k, v) in K2_TARFILES.items() if k in limits}
    else:
        compagnes = K2_TARFILES

    for compagne, nbrfiles in compagnes.items():
        click.echo('downloading {} dataset'.format(compagne))
        for nbrfile in range(1, nbrfiles):

           
            filename = 'public_{}_long_{}'.format(compagne, nbrfile)
            outfolder = lightcurvesfolder + filename

             # checking if already downloaded
            if os.path.exists(outfolder):
                click.echo('{} already downloaded, moving on'.format(filename))
                continue
            else:
                os.makedirs(outfolder)

            url = K2_URL + compagne + "/" + filename + ".tgz"
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


        click.echo('compagne {} done!'.format(compagne))

    click.echo('all compagnes are fetched, generating csv')

@cli.command()
def update():
    click.echo('Updating the database...')

if __name__ == '__main__':
    cli()
    