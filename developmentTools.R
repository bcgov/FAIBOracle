
rmarkdown::render("README.rmd", output_format = "md_document")
devtools::document()
devtools::check(document = TRUE, args = "--no-multiarch")
devtools::check(args = "--no-multiarch")
system("R CMD Rd2pdf --force --output=./documents/Manual.pdf ." )
devtools::build(args = "--no-multiarch")
