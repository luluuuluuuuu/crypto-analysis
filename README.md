# crypto-analysis

## Final Report

Open the pdf file `<root>/Report.pdf`

## Prerequisite

- [Python](https://www.python.org/downloads/)

- [Jupyter Notebook](http://jupyter.org/install)

- [Java](https://docs.oracle.com/javase/8/docs/technotes/guides/install/install_overview.html)

- [Maven](https://maven.apache.org/install.html)

- [Docker](https://docs.docker.com/install/)

- [Docker Compose](https://docs.docker.com/compose/install/)

- [LaTeX](https://www.latex-project.org/get/)

## Development

### Database

Create a file called `.env-dev` in the `<root>` directory and insert the following configuration.

```bash
$ POSTGRES_DB=postgres
$ POSTGRES_USER=postgres
$ POSTGRES_PASSWORD=Crypto01
```

Execute the following commands.

```bash
$ cd <root>/
$ docker-compose up
```

### Server

Execute the following commands.

```bash
$ cd <root>/parent/
$ mvn clean install
$ cd <root>/analysis/
$ mvn exec:Java
```

### Experiment

Execute the following commands, and then open the `var_crypto.ipynb` or `var_stock.ipynb` file in the browser.

```bash
$ cd <root>/experiment/
$ jupyter notebook
```

### Report

Execute the following commands to compile the LaTeX file.

```bash
$ cd <root>/report/
$ pdflatex Report.tex
$ cp Report.pdf ../Report.pdf
```
