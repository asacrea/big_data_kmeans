DocumentationDep=Logarithm.pdf\
				 LogarithmPos.pdf\
				 Documentation.bib\
				 Documentation.tex

Documentation.pdf: $(DocumentationDep)
	pdflatex Documentation.tex
	bibtex Documentation
	pdflatex Documentation.tex
	pdflatex Documentation.tex
	pdflatex Documentation.tex


Logarithm.pdf: Logarithm.tex
	pdflatex Logarithm.tex

LogarithmPos.pdf: LogarithmPos.tex
	pdflatex LogarithmPos.tex

clean:
	rm -f *.pdf
	rm -f *.nav *.snm *.toc *.out *.dvi *.blg *.bbl *.aux *.log

exe:
	evince Documentation.pdf &
