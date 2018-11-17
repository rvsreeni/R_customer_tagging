#R wrapper script

flist <- list.files(path = "./input", pattern = "*.csv")
for (fl in flist) {
ipathval = c(".","input", fl)
ipathfile = paste0(ipathval,collapse="/")
print(system.time(suppressWarnings(source("./src/CDBcleanup_fullpar_main.R"))))
}
