
rmarkdown::render("README.rmd", output_format = "md_document")
devtools::document()
system("R CMD Rd2pdf --force --output=./vignettes/Manual.pdf ." )
devtools::install(args = "--no-multiarch")
devtools::check(document = TRUE, args = "--no-multiarch")
devtools::build(args = "--no-multiarch")


install.packages("D:/GitHub/FAIBOracle_0.1.tar.gz",
                 repos = NULL, INSTALL_opts = "--no-multiarch")


