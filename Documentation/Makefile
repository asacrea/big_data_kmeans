DocumentationDep=Logarithm.pdf\
				 LogarithmPos.pdf\
				 ResultsSpark.pdf\
				 ResultsCombined.pdf\
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

ResultsSpark.pdf: ResultsSpark.tex Local.dat Distributed2.dat Distributed3.dat Distributed4.dat
	pdflatex ResultsSpark.tex

ResultsCombined.pdf: ResultsCombined.tex Serial.dat Parallel.dat Local.dat Distributed2.dat Distributed3.dat Distributed4.dat
	pdflatex ResultsCombined.tex

clean:
	rm -f *.pdf
	rm -f *.nav *.snm *.toc *.out *.dvi *.blg *.bbl *.aux *.log

exe:
	evince Documentation.pdf &
