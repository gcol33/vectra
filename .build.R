setwd("C:/Users/Gilles Colling/Documents/dev/vectra")
path <- devtools::build()
cat("TARBALL:", path, "\n")
