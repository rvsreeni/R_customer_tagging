####################################################################################################################################
#
# This R Main script does the following -
# Reads in the Prism CDB Full input file
# Eliminate Duplicates (on Customer Number) - write Duplicates to output file
# Validate - customer Name, Phone - write Invalid entries to output files
# Match Tagging logic
#    Iteration #1    - Sort on Phone#, divide the input into 40 chunks, process them in parallel(4 chunks at a time)
#                    - Calls CDBcleanup_fullpar_phone_match_upd (which calls ParFn_phone_bndchk, ParFn_phone_match)
#                      to perform Match tagging logic and update Masterid, Description
#                    - Calls ParFn_phone_bndchk – check if same Phone# is present in the boundaries of successive chunks  
#                      and if so, readjust the chunks  so that Phone# is present in only one of the chunks
#                    - Calls function ParFn_phone_match to do do Match Tagging on Phone, Name, Address
#                    - Update (MasterId, Description)  
#    Iteration #2.1  - Sort on Name, divide the input into 40 chunks, process them in parallel(4 chunks at a time)
#	             - Calls CDBcleanup_fullpar_name_match_upd
#	             - Calls ParFn_name_bndchk
#	             - Calls function ParFn_name_match to do Match Tagging on Phone, Name, Address
#	             - Update (MasterId, Description) 
#    Iteration #2.2  - Sort on Reversed Name, divide the input into 40 chunks, process them in parallel(4 chunks at a time)
#	             - Calls CDBcleanup_fullpar_name_match_upd
#	             - Calls ParFn_name_bndchk
#	             - Calls function ParFn_name_match to do Match Tagging on Phone, Reversed Name, Address
#	             - Update (MasterId, Description) 
#    Iteration #3.1  - Sort on Address, divide the input into 40 chunks, process them in parallel(4 chunks at a time)
#                    - Calls CDBcleanup_fullpar_addr_match_upd
#                    - Calls ParFn_addr_bndchk
#                    - Calls function ParFn_addr_match to do Match Tagging on Phone, Name, Address               
#                    - Update (MasterId, Description) 
#    Iteration #3.2  - Sort on Reversed Address, divide the input into 40 chunks, process them in parallel(4 chunks at a time)
#                    - Calls CDBcleanup_fullpar_addr_match_upd
#                    - Calls ParFn_addr_bndchk
#                    - Calls function ParFn_addr_match to do Match Tagging on Phone, Name, Reversed Address    
#                    - Update (MasterId, Description) 
# Link Masterid of related customers
#     - Split data Region wise into separate chunks, process them in parallel(4 chunks at a time)
#     - Calls function ParFullFn_linkmid to link Masterid of related customers
# Write to Output file (input file fields, plus masterid, description) 
#
#####################################################################################################################################

###### Begin Script ######

#Packages required
# install.packages("stringdist")
# install.packages("stringr")
# install.packages("doParallel")

library(stringdist)
library(stringr)
library(parallel)
library(doParallel)
library(foreach)

#Detecting & assigning all available processors
detectCores(all.tests = FALSE, logical = TRUE)
getDoParWorkers()
registerDoParallel(cores=detectCores(all.tests = FALSE, logical = TRUE))
print("getDoParWorkers")
print(getDoParWorkers())

#set current working directory
cwd <- getwd()

#Load Match functions(Phone, Name, Address)
source("./src/CDBcleanup_par_match_func.R")

#Load link MasterId function
source("./src/CDBcleanup_fullpar_linkmid.R")

#Read Prism CDB Full Input file
#basedata <- read.csv(ipathfile, header=TRUE, fileEncoding="latin1", stringsAsFactors=FALSE)
basedata <- read.csv(ipathfile, header=TRUE, stringsAsFactors=FALSE)

print("Execution started")
print("-----------------")

#Format system date removing spaces
sys_date <- gsub(" ","", date())

#Formatting Date
basedata$LAST_UPDATE_DATE_NEW <- as.Date(basedata$LAST_UPDATE_DATE,"%d/%m/%Y")

#Eliminate duplicate data (on Customer Number), write Duplicates to output file
basedata <- basedata[order(basedata$CUSTOMER_NUMBER, basedata$LAST_UPDATE_DATE_NEW, decreasing = TRUE),]
ofileval = c("ID Duplicate Iteration", sys_date)
tochg <- c("[[:punct:]]","[[:space:]]")
tochgdlm <- paste0(tochg, collapse="|")
ofilename <- gsub(tochgdlm,"", ofileval)
ofilename <- c(ofilename, "csv")
ofilename = paste0(ofilename, collapse=".")
opathval = c(cwd,"output", ofilename)
opathfile = paste0(opathval, collapse="/")
write.csv(basedata[duplicated(basedata$CUSTOMER_NUMBER), ], opathfile)
basedata <- basedata[!duplicated(basedata$CUSTOMER_NUMBER), ]

#Row length
row.length <- nrow(basedata)
print(row.length)

#Set Default value (for MasterId)
basedata$masteridnew = basedata$CUSTOMER_NUMBER

#Formatting Phone number
tochg <- c("[[:punct:]]","[[:space:]]","[[:alpha:]]")
tochgdlm <- paste0(tochg,collapse="|")
basedata$PHONE_NUMBER_new <- gsub(tochgdlm, "", basedata$PHONE_NUMBER_1)
basedata$PHONE_NUMBER_new[is.na(basedata$PHONE_NUMBER_new)] <- 0

#Formatting Address
basedata$ADDRESS_formatted <- tolower(basedata$ADDRESS)
tochg <- c("-","m/s","m/s ","mr[.]+","ms[.]+","m/s[.]+","mrs[.]+","w/o","w/O","s/o","s/O","c/o","c/O","d/o","d/O","\\s","[[:punct:]]")
tochgdlm <- paste0(tochg,collapse="|")
basedata$ADDRESS_formatted  <- gsub(tochgdlm,"",basedata$ADDRESS_formatted)
basedata$ADDRESS_formatted <- str_trim(basedata$ADDRESS_formatted)
basedata$ADDRESS_new <- iconv(basedata$ADDRESS_formatted, "latin1", "ASCII", sub="")

#Formatting customer name
basedata$CUSTOMER_NAME_formatted <- tolower(basedata$CUSTOMER_NAME)
tochg <- c("-","mr ","ms ","m/s","m/s ","mrs ","mr[.]+","ms[.]+","m/s[.]+","mrs[.]+","\\s","[[:punct:]]")
tochgdlm <- paste0(tochg,collapse="|")
basedata$CUSTOMER_NAME_formatted <- gsub(tochgdlm,"",basedata$CUSTOMER_NAME_formatted)
basedata$CUSTOMER_NAME_formatted <- str_trim(basedata$CUSTOMER_NAME_formatted)
basedata$CUSTOMER_NAME_new <- iconv(basedata$CUSTOMER_NAME_formatted, "latin1", "ASCII", sub="")

#Set Default value (for Description)
basedata$description="Unique customer"

#Wrong Phone detection
PHONE_NUMBER_new <- basedata$PHONE_NUMBER_new
Description <- basedata$description 
for(i in 1:(row.length)){
#Remove leading Zeros
  PHONE_NUMBER_new[i] <- substr(PHONE_NUMBER_new[i], regexpr("[^0]",PHONE_NUMBER_new[i]), nchar(PHONE_NUMBER_new[i]))
#Validate Phone Number
  if(PHONE_NUMBER_new[i]<100000|PHONE_NUMBER_new[i]>999999999999|grepl(9999999999,PHONE_NUMBER_new[i])|grepl(1111111111,PHONE_NUMBER_new[i])|grepl(2222222222,PHONE_NUMBER_new[i])|
     grepl(3333333333,PHONE_NUMBER_new[i])|grepl(4444444444,PHONE_NUMBER_new[i])|grepl(5555555555,PHONE_NUMBER_new[i])|grepl(6666666666,PHONE_NUMBER_new[i])|
     grepl(7777777777,PHONE_NUMBER_new[i])|grepl(8888888888,PHONE_NUMBER_new[i])|grepl(1234567890,PHONE_NUMBER_new[i])|grepl(0123456789,PHONE_NUMBER_new[i]))
       { Description[i]="Wrong phone no" }
}
basedata$PHONE_NUMBER_new <- PHONE_NUMBER_new
basedata$description <- Description 
Wrongphonenumber <- subset(basedata,basedata$description=="Wrong phone no")
ofileval = c("Wrongphoneiteration", sys_date)
tochg <- c("[[:punct:]]","[[:space:]]")
tochgdlm <- paste0(tochg, collapse="|")
ofilename <- gsub(tochgdlm,"", ofileval)
ofilename <- c(ofilename, "csv")
ofilename = paste0(ofilename, collapse=".")
opathval = c(cwd,"output", ofilename)
opathfile = paste0(opathval, collapse="/")
write.csv(Wrongphonenumber, opathfile)

#Wrong Name detection
CUSTOMER_NAME_new <- basedata$CUSTOMER_NAME_new
Description <- basedata$description
for(i in 1:(row.length)){
  if(nchar(as.character(CUSTOMER_NAME_new[i])) < 3)
   { Description[i]="Wrong name" }
}
basedata$description <- Description
Wrongname <- subset(basedata,basedata$description=="Wrong name")
ofileval = c("Wrongnameiteration", sys_date)
tochg <- c("[[:punct:]]","[[:space:]]")
tochgdlm <- paste0(tochg, collapse="|")
ofilename <- gsub(tochgdlm,"", ofileval)
ofilename <- c(ofilename, "csv")
ofilename = paste0(ofilename, collapse=".")
opathval = c(cwd,"output", ofilename)
opathfile = paste0(opathval, collapse="/")
write.csv(Wrongname, opathfile)

#Segregating dataframes
basedatawrt  <- basedata[,c("CUSTOMER_NUMBER","CUSTOMER_NAME_new","PHONE_NUMBER_new", "ADDRESS_new","masteridnew","description")]
basedatarem1 <- basedata[,c("CUSTOMER_NAME","PHONE_NUMBER_1", "ADDRESS","REGION","AREA","DEALERSHIP","ATTRIBUTE","ATTRIBUTE5","CITY","HUB")]
basedatarem2 <- basedata[,c("SALESEXECUTIVE1","SALESEXECNAME1","SALESEXECUTIVE2","SALESEXECNAME2","SALESEXECUTIVE3","SALESEXECNAME3","SALESEXECUTIVE4","SALESEXECNAME4","SALESEXECUTIVE5")]
basedatarem3 <- basedata[,c("SALESEXECNAME5","SALESEXECUTIVE6","SALESEXECNAME6","APPLICATION","SEGMENT","OTHERFLEETSIZE","ALFLEETSIZE","PROXIMITY","CREATION_DATE","LAST_UPDATE_DATE")]

#Row length
lenbdwrt <- nrow(basedatawrt)
intvl <- as.integer(lenbdwrt/40) + 1
print("Record Count")
print(lenbdwrt)
print(intvl)

#Calculate start, end position for 40 chunks
start1 <- (((1-1)*intvl)+1)
end1 <- (1*intvl)
start2 <- (((2-1)*intvl)+1)
end2 <- (2*intvl)
start3 <- (((3-1)*intvl)+1)
end3 <- (3*intvl)
start4 <- (((4-1)*intvl)+1)
end4 <- (4*intvl)
start5 <- (((5-1)*intvl)+1)
end5 <- (5*intvl)
start6 <- (((6-1)*intvl)+1)
end6 <- (6*intvl)
start7 <- (((7-1)*intvl)+1)
end7 <- (7*intvl)
start8 <- (((8-1)*intvl)+1)
end8 <- (8*intvl)
start9 <- (((9-1)*intvl)+1)
end9 <- (9*intvl)
start10 <- (((10-1)*intvl)+1)
end10 <- (10*intvl)
start11 <- (((11-1)*intvl)+1)
end11 <- (11*intvl)
start12 <- (((12-1)*intvl)+1)
end12 <- (12*intvl)
start13 <- (((13-1)*intvl)+1)
end13 <- (13*intvl)
start14 <- (((14-1)*intvl)+1)
end14 <- (14*intvl)
start15 <- (((15-1)*intvl)+1)
end15 <- (15*intvl)
start16 <- (((16-1)*intvl)+1)
end16 <- (16*intvl)
start17 <- (((17-1)*intvl)+1)
end17 <- (17*intvl)
start18 <- (((18-1)*intvl)+1)
end18 <- (18*intvl)
start19 <- (((19-1)*intvl)+1)
end19 <- (19*intvl)
start20 <- (((20-1)*intvl)+1)
end20 <- (20*intvl)
start21 <- (((21-1)*intvl)+1)
end21 <- (21*intvl)
start22 <- (((22-1)*intvl)+1)
end22 <- (22*intvl)
start23 <- (((23-1)*intvl)+1)
end23 <- (23*intvl)
start24 <- (((24-1)*intvl)+1)
end24 <- (24*intvl)
start25 <- (((25-1)*intvl)+1)
end25 <- (25*intvl)
start26 <- (((26-1)*intvl)+1)
end26 <- (26*intvl)
start27 <- (((27-1)*intvl)+1)
end27 <- (27*intvl)
start28 <- (((28-1)*intvl)+1)
end28 <- (28*intvl)
start29 <- (((29-1)*intvl)+1)
end29 <- (29*intvl)
start30 <- (((30-1)*intvl)+1)
end30 <- (30*intvl)
start31 <- (((31-1)*intvl)+1)
end31 <- (31*intvl)
start32 <- (((32-1)*intvl)+1)
end32 <- (32*intvl)
start33 <- (((33-1)*intvl)+1)
end33 <- (33*intvl)
start34 <- (((34-1)*intvl)+1)
end34 <- (34*intvl)
start35 <- (((35-1)*intvl)+1)
end35 <- (35*intvl)
start36 <- (((36-1)*intvl)+1)
end36 <- (36*intvl)
start37 <- (((37-1)*intvl)+1)
end37 <- (37*intvl)
start38 <- (((38-1)*intvl)+1)
end38 <- (38*intvl)
start39 <- (((39-1)*intvl)+1)
end39 <- (39*intvl)
start40 <- (((40-1)*intvl)+1)
end40 <- (lenbdwrt)


#Iteration #1 - Process the dataframe (sorted by Phone)
#Iteration #1 - Process the dataframe (sorted by Phone)
#Iteration #1 - Process the dataframe (sorted by Phone)

basedatawrtn <- basedatawrt[order(basedatawrt$PHONE_NUMBER_new,-basedatawrt$CUSTOMER_NUMBER),]

outbdwrtn <- NULL

#Process First set of 2 chunks
basedatawrt12 = basedatawrtn[start1:end1,]
basedatawrt22 = basedatawrtn[start2:end2,]

print("Iteration #1.1")
source("./src/CDBcleanup_fullpar_phone_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Second set of 2 chunks
basedatawrt12 = basedatawrtn[start3:end3,]
basedatawrt22 = basedatawrtn[start4:end4,]

print("Iteration #1.2")
source("./src/CDBcleanup_fullpar_phone_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Third set of 2 chunks
basedatawrt12 = basedatawrtn[start5:end5,]
basedatawrt22 = basedatawrtn[start6:end6,]

print("Iteration #1.3")
source("./src/CDBcleanup_fullpar_phone_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Fourth set of 2 chunks
basedatawrt12 = basedatawrtn[start7:end7,]
basedatawrt22 = basedatawrtn[start8:end8,]

print("Iteration #1.4")
source("./src/CDBcleanup_fullpar_phone_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Fifth set of 2 chunks
basedatawrt12 = basedatawrtn[start9:end9,]
basedatawrt22 = basedatawrtn[start10:end10,]

print("Iteration #1.5")
source("./src/CDBcleanup_fullpar_phone_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Sixth set of 2 chunks
basedatawrt12 = basedatawrtn[start11:end11,]
basedatawrt22 = basedatawrtn[start12:end12,]

print("Iteration #1.6")
source("./src/CDBcleanup_fullpar_phone_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Seventh set of 2 chunks
basedatawrt12 = basedatawrtn[start13:end13,]
basedatawrt22 = basedatawrtn[start14:end14,]

print("Iteration #1.7")
source("./src/CDBcleanup_fullpar_phone_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Eigth set of 2 chunks
basedatawrt12 = basedatawrtn[start15:end15,]
basedatawrt22 = basedatawrtn[start16:end16,]

print("Iteration #1.8")
source("./src/CDBcleanup_fullpar_phone_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Ninth set of 2 chunks
basedatawrt12 = basedatawrtn[start17:end17,]
basedatawrt22 = basedatawrtn[start18:end18,]

print("Iteration #1.9")
source("./src/CDBcleanup_fullpar_phone_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Tenth set of 2 chunks
basedatawrt12 = basedatawrtn[start19:end19,]
basedatawrt22 = basedatawrtn[start20:end20,]

print("Iteration #1.10")
source("./src/CDBcleanup_fullpar_phone_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Eleventh set of 2 chunks
basedatawrt12 = basedatawrtn[start21:end21,]
basedatawrt22 = basedatawrtn[start22:end22,]

print("Iteration #1.11")
source("./src/CDBcleanup_fullpar_phone_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Twelveth set of 2 chunks
basedatawrt12 = basedatawrtn[start23:end23,]
basedatawrt22 = basedatawrtn[start24:end24,]

print("Iteration #1.12")
source("./src/CDBcleanup_fullpar_phone_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Thirteenth set of 2 chunks
basedatawrt12 = basedatawrtn[start25:end25,]
basedatawrt22 = basedatawrtn[start26:end26,]

print("Iteration #1.13")
source("./src/CDBcleanup_fullpar_phone_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Fourteenth set of 2 chunks
basedatawrt12 = basedatawrtn[start27:end27,]
basedatawrt22 = basedatawrtn[start28:end28,]

print("Iteration #1.14")
source("./src/CDBcleanup_fullpar_phone_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Fifteenth set of 2 chunks
basedatawrt12 = basedatawrtn[start29:end29,]
basedatawrt22 = basedatawrtn[start30:end30,]

print("Iteration #1.15")
source("./src/CDBcleanup_fullpar_phone_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Sixteenth set of 2 chunks
basedatawrt12 = basedatawrtn[start31:end31,]
basedatawrt22 = basedatawrtn[start32:end32,]

print("Iteration #1.16")
source("./src/CDBcleanup_fullpar_phone_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Seventeenth set of 2 chunks
basedatawrt12 = basedatawrtn[start33:end33,]
basedatawrt22 = basedatawrtn[start34:end34,]

print("Iteration #1.17")
source("./src/CDBcleanup_fullpar_phone_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Eighteenth set of 2 chunks
basedatawrt12 = basedatawrtn[start35:end35,]
basedatawrt22 = basedatawrtn[start36:end36,]

print("Iteration #1.18")
source("./src/CDBcleanup_fullpar_phone_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Nineteenth set of 2 chunks
basedatawrt12 = basedatawrtn[start37:end37,]
basedatawrt22 = basedatawrtn[start38:end38,]

print("Iteration #1.19")
source("./src/CDBcleanup_fullpar_phone_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Twentieth set of 2 chunks
basedatawrt12 = basedatawrtn[start39:end39,]
basedatawrt22 = basedatawrtn[start40:end40,]

print("Iteration #1.20")
source("./src/CDBcleanup_fullpar_phone_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#write.csv(outbdwrtn, "outbdwrtn1.csv")

#Iteration #2.1 - Process the resulting dataframe from iteration #1 (sorted by Name)
#Iteration #2.1 - Process the resulting dataframe from iteration #1 (sorted by Name)
#Iteration #2.1 - Process the resulting dataframe from iteration #1 (sorted by Name)

basedatawrtn <- outbdwrtn
basedatawrtn <- basedatawrtn[order(basedatawrtn$CUSTOMER_NAME_new,-basedatawrtn$CUSTOMER_NUMBER),]

outbdwrtn <- NULL

#Process First set of 2 chunks
basedatawrt12 = basedatawrtn[start1:end1,]
basedatawrt22 = basedatawrtn[start2:end2,]

print("Iteration #2.1.1")
source("./src/CDBcleanup_fullpar_name_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Second set of 2 chunks
basedatawrt12 = basedatawrtn[start3:end3,]
basedatawrt22 = basedatawrtn[start4:end4,]

print("Iteration #2.1.2")
source("./src/CDBcleanup_fullpar_name_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Third set of 2 chunks
basedatawrt12 = basedatawrtn[start5:end5,]
basedatawrt22 = basedatawrtn[start6:end6,]

print("Iteration #2.1.3")
source("./src/CDBcleanup_fullpar_name_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Fourth set of 2 chunks
basedatawrt12 = basedatawrtn[start7:end7,]
basedatawrt22 = basedatawrtn[start8:end8,]

print("Iteration #2.1.4")
source("./src/CDBcleanup_fullpar_name_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Fifth set of 2 chunks
basedatawrt12 = basedatawrtn[start9:end9,]
basedatawrt22 = basedatawrtn[start10:end10,]

print("Iteration #2.1.5")
source("./src/CDBcleanup_fullpar_name_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Sixth set of 2 chunks
basedatawrt12 = basedatawrtn[start11:end11,]
basedatawrt22 = basedatawrtn[start12:end12,]

print("Iteration #2.1.6")
source("./src/CDBcleanup_fullpar_name_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Seventh set of 2 chunks
basedatawrt12 = basedatawrtn[start13:end13,]
basedatawrt22 = basedatawrtn[start14:end14,]

print("Iteration #2.1.7")
source("./src/CDBcleanup_fullpar_name_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Eigth set of 2 chunks
basedatawrt12 = basedatawrtn[start15:end15,]
basedatawrt22 = basedatawrtn[start16:end16,]

print("Iteration #2.1.8")
source("./src/CDBcleanup_fullpar_name_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Ninth set of 2 chunks
basedatawrt12 = basedatawrtn[start17:end17,]
basedatawrt22 = basedatawrtn[start18:end18,]

print("Iteration #2.1.9")
source("./src/CDBcleanup_fullpar_name_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Tenth set of 2 chunks
basedatawrt12 = basedatawrtn[start19:end19,]
basedatawrt22 = basedatawrtn[start20:end20,]

print("Iteration #2.1.10")
source("./src/CDBcleanup_fullpar_name_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Eleventh set of 2 chunks
basedatawrt12 = basedatawrtn[start21:end21,]
basedatawrt22 = basedatawrtn[start22:end22,]

print("Iteration #2.1.11")
source("./src/CDBcleanup_fullpar_name_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Twelveth set of 2 chunks
basedatawrt12 = basedatawrtn[start23:end23,]
basedatawrt22 = basedatawrtn[start24:end24,]

print("Iteration #2.1.12")
source("./src/CDBcleanup_fullpar_name_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Thirteenth set of 2 chunks
basedatawrt12 = basedatawrtn[start25:end25,]
basedatawrt22 = basedatawrtn[start26:end26,]

print("Iteration #2.1.13")
source("./src/CDBcleanup_fullpar_name_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Fourteenth set of 2 chunks
basedatawrt12 = basedatawrtn[start27:end27,]
basedatawrt22 = basedatawrtn[start28:end28,]

print("Iteration #2.1.14")
source("./src/CDBcleanup_fullpar_name_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Fifteenth set of 2 chunks
basedatawrt12 = basedatawrtn[start29:end29,]
basedatawrt22 = basedatawrtn[start30:end30,]

print("Iteration #2.1.15")
source("./src/CDBcleanup_fullpar_name_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Sixteenth set of 2 chunks
basedatawrt12 = basedatawrtn[start31:end31,]
basedatawrt22 = basedatawrtn[start32:end32,]

print("Iteration #2.1.16")
source("./src/CDBcleanup_fullpar_name_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Seventeenth set of 2 chunks
basedatawrt12 = basedatawrtn[start33:end33,]
basedatawrt22 = basedatawrtn[start34:end34,]

print("Iteration #2.1.17")
source("./src/CDBcleanup_fullpar_name_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Eighteenth set of 2 chunks
basedatawrt12 = basedatawrtn[start35:end35,]
basedatawrt22 = basedatawrtn[start36:end36,]

print("Iteration #2.1.18")
source("./src/CDBcleanup_fullpar_name_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Nineteenth set of 2 chunks
basedatawrt12 = basedatawrtn[start37:end37,]
basedatawrt22 = basedatawrtn[start38:end38,]

print("Iteration #2.1.19")
source("./src/CDBcleanup_fullpar_name_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Twentieth set of 2 chunks
basedatawrt12 = basedatawrtn[start39:end39,]
basedatawrt22 = basedatawrtn[start40:end40,]

print("Iteration #2.1.20")
source("./src/CDBcleanup_fullpar_name_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#write.csv(outbdwrtn, "outbdwrtn2-1.csv")


#Iteration #2.2 - Process the resulting dataframe from iteration #2.1 (sorted by Reversed Name)
#Iteration #2.2 - Process the resulting dataframe from iteration #2.1 (sorted by Reversed Name)
#Iteration #2.2 - Process the resulting dataframe from iteration #2.1 (sorted by Reversed Name)

basedatawrtn <- outbdwrtn

#Reverse the name
Name <- basedatawrtn$CUSTOMER_NAME_new
RevName <- sapply(Name, function(x) { paste(rev(substring(x,1:nchar(x),1:nchar(x))),collapse="") })
names(RevName) <- NULL
basedatawrtn$CUSTOMER_NAME_new <- RevName

basedatawrtn <- basedatawrtn[order(basedatawrtn$CUSTOMER_NAME_new,-basedatawrtn$CUSTOMER_NUMBER),]

outbdwrtn <- NULL

#Process First set of 2 chunks
basedatawrt12 = basedatawrtn[start1:end1,]
basedatawrt22 = basedatawrtn[start2:end2,]

print("Iteration #2.2.1")
source("./src/CDBcleanup_fullpar_name_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Second set of 2 chunks
basedatawrt12 = basedatawrtn[start3:end3,]
basedatawrt22 = basedatawrtn[start4:end4,]

print("Iteration #2.2.2")
source("./src/CDBcleanup_fullpar_name_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Third set of 2 chunks
basedatawrt12 = basedatawrtn[start5:end5,]
basedatawrt22 = basedatawrtn[start6:end6,]

print("Iteration #2.2.3")
source("./src/CDBcleanup_fullpar_name_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Fourth set of 2 chunks
basedatawrt12 = basedatawrtn[start7:end7,]
basedatawrt22 = basedatawrtn[start8:end8,]

print("Iteration #2.2.4")
source("./src/CDBcleanup_fullpar_name_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Fifth set of 2 chunks
basedatawrt12 = basedatawrtn[start9:end9,]
basedatawrt22 = basedatawrtn[start10:end10,]

print("Iteration #2.2.5")
source("./src/CDBcleanup_fullpar_name_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Sixth set of 2 chunks
basedatawrt12 = basedatawrtn[start11:end11,]
basedatawrt22 = basedatawrtn[start12:end12,]

print("Iteration #2.2.6")
source("./src/CDBcleanup_fullpar_name_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Seventh set of 2 chunks
basedatawrt12 = basedatawrtn[start13:end13,]
basedatawrt22 = basedatawrtn[start14:end14,]

print("Iteration #2.2.7")
source("./src/CDBcleanup_fullpar_name_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Eigth set of 2 chunks
basedatawrt12 = basedatawrtn[start15:end15,]
basedatawrt22 = basedatawrtn[start16:end16,]

print("Iteration #2.2.8")
source("./src/CDBcleanup_fullpar_name_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Ninth set of 2 chunks
basedatawrt12 = basedatawrtn[start17:end17,]
basedatawrt22 = basedatawrtn[start18:end18,]

print("Iteration #2.2.9")
source("./src/CDBcleanup_fullpar_name_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Tenth set of 2 chunks
basedatawrt12 = basedatawrtn[start19:end19,]
basedatawrt22 = basedatawrtn[start20:end20,]

print("Iteration #2.2.10")
source("./src/CDBcleanup_fullpar_name_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Eleventh set of 2 chunks
basedatawrt12 = basedatawrtn[start21:end21,]
basedatawrt22 = basedatawrtn[start22:end22,]

print("Iteration #2.2.11")
source("./src/CDBcleanup_fullpar_name_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Twelveth set of 2 chunks
basedatawrt12 = basedatawrtn[start23:end23,]
basedatawrt22 = basedatawrtn[start24:end24,]

print("Iteration #2.2.12")
source("./src/CDBcleanup_fullpar_name_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Thirteenth set of 2 chunks
basedatawrt12 = basedatawrtn[start25:end25,]
basedatawrt22 = basedatawrtn[start26:end26,]

print("Iteration #2.2.13")
source("./src/CDBcleanup_fullpar_name_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Fourteenth set of 2 chunks
basedatawrt12 = basedatawrtn[start27:end27,]
basedatawrt22 = basedatawrtn[start28:end28,]

print("Iteration #2.2.14")
source("./src/CDBcleanup_fullpar_name_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Fifteenth set of 2 chunks
basedatawrt12 = basedatawrtn[start29:end29,]
basedatawrt22 = basedatawrtn[start30:end30,]

print("Iteration #2.2.15")
source("./src/CDBcleanup_fullpar_name_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Sixteenth set of 2 chunks
basedatawrt12 = basedatawrtn[start31:end31,]
basedatawrt22 = basedatawrtn[start32:end32,]

print("Iteration #2.2.16")
source("./src/CDBcleanup_fullpar_name_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Seventeenth set of 2 chunks
basedatawrt12 = basedatawrtn[start33:end33,]
basedatawrt22 = basedatawrtn[start34:end34,]

print("Iteration #2.2.17")
source("./src/CDBcleanup_fullpar_name_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Eighteenth set of 2 chunks
basedatawrt12 = basedatawrtn[start35:end35,]
basedatawrt22 = basedatawrtn[start36:end36,]

print("Iteration #2.2.18")
source("./src/CDBcleanup_fullpar_name_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Nineteenth set of 2 chunks
basedatawrt12 = basedatawrtn[start37:end37,]
basedatawrt22 = basedatawrtn[start38:end38,]

print("Iteration #2.2.19")
source("./src/CDBcleanup_fullpar_name_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Twentieth set of 2 chunks
basedatawrt12 = basedatawrtn[start39:end39,]
basedatawrt22 = basedatawrtn[start40:end40,]

print("Iteration #2.2.20")
source("./src/CDBcleanup_fullpar_name_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Reverse the reversed Name to correct format
Name <- outbdwrtn$CUSTOMER_NAME_new
RevName <- sapply(Name, function(x) { paste(rev(substring(x,1:nchar(x),1:nchar(x))),collapse="") })
names(RevName) <- NULL
outbdwrtn$CUSTOMER_NAME_new <- RevName

#write.csv(outbdwrtn, "outbdwrtn2_2.csv")


#Iteration #3.1 - Process the resulting dataframe from iteration #2.2 (sorted by Address)
#Iteration #3.1 - Process the resulting dataframe from iteration #2.2 (sorted by Address)
#Iteration #3.1 - Process the resulting dataframe from iteration #2.2 (sorted by Address)

basedatawrtn <- outbdwrtn
basedatawrtn <- basedatawrtn[order(basedatawrtn$ADDRESS_new,-basedatawrtn$CUSTOMER_NUMBER),]

outbdwrtn <- NULL

#Process First set of 2 chunks
basedatawrt12 = basedatawrtn[start1:end1,]
basedatawrt22 = basedatawrtn[start2:end2,]

print("Iteration #3.1.1")
source("./src/CDBcleanup_fullpar_addr_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Second set of 2 chunks
basedatawrt12 = basedatawrtn[start3:end3,]
basedatawrt22 = basedatawrtn[start4:end4,]

print("Iteration #3.1.2")
source("./src/CDBcleanup_fullpar_addr_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Third set of 2 chunks
basedatawrt12 = basedatawrtn[start5:end5,]
basedatawrt22 = basedatawrtn[start6:end6,]

print("Iteration #3.1.3")
source("./src/CDBcleanup_fullpar_addr_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Fourth set of 2 chunks
basedatawrt12 = basedatawrtn[start7:end7,]
basedatawrt22 = basedatawrtn[start8:end8,]

print("Iteration #3.1.4")
source("./src/CDBcleanup_fullpar_addr_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Fifth set of 2 chunks
basedatawrt12 = basedatawrtn[start9:end9,]
basedatawrt22 = basedatawrtn[start10:end10,]

print("Iteration #3.1.5")
source("./src/CDBcleanup_fullpar_addr_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Sixth set of 2 chunks
basedatawrt12 = basedatawrtn[start11:end11,]
basedatawrt22 = basedatawrtn[start12:end12,]

print("Iteration #3.1.6")
source("./src/CDBcleanup_fullpar_addr_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Seventh set of 2 chunks
basedatawrt12 = basedatawrtn[start13:end13,]
basedatawrt22 = basedatawrtn[start14:end14,]

print("Iteration #3.1.7")
source("./src/CDBcleanup_fullpar_addr_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Eigth set of 2 chunks
basedatawrt12 = basedatawrtn[start15:end15,]
basedatawrt22 = basedatawrtn[start16:end16,]

print("Iteration #3.1.8")
source("./src/CDBcleanup_fullpar_addr_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Ninth set of 2 chunks
basedatawrt12 = basedatawrtn[start17:end17,]
basedatawrt22 = basedatawrtn[start18:end18,]

print("Iteration #3.1.9")
source("./src/CDBcleanup_fullpar_addr_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Tenth set of 2 chunks
basedatawrt12 = basedatawrtn[start19:end19,]
basedatawrt22 = basedatawrtn[start20:end20,]

print("Iteration #3.1.10")
source("./src/CDBcleanup_fullpar_addr_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Eleventh set of 2 chunks
basedatawrt12 = basedatawrtn[start21:end21,]
basedatawrt22 = basedatawrtn[start22:end22,]

print("Iteration #3.1.11")
source("./src/CDBcleanup_fullpar_addr_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Twelveth set of 2 chunks
basedatawrt12 = basedatawrtn[start23:end23,]
basedatawrt22 = basedatawrtn[start24:end24,]

print("Iteration #3.1.12")
source("./src/CDBcleanup_fullpar_addr_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Thirteenth set of 2 chunks
basedatawrt12 = basedatawrtn[start25:end25,]
basedatawrt22 = basedatawrtn[start26:end26,]

print("Iteration #3.1.13")
source("./src/CDBcleanup_fullpar_addr_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Fourteenth set of 2 chunks
basedatawrt12 = basedatawrtn[start27:end27,]
basedatawrt22 = basedatawrtn[start28:end28,]

print("Iteration #3.1.14")
source("./src/CDBcleanup_fullpar_addr_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Fifteenth set of 2 chunks
basedatawrt12 = basedatawrtn[start29:end29,]
basedatawrt22 = basedatawrtn[start30:end30,]

print("Iteration #3.1.15")
source("./src/CDBcleanup_fullpar_addr_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Sixteenth set of 2 chunks
basedatawrt12 = basedatawrtn[start31:end31,]
basedatawrt22 = basedatawrtn[start32:end32,]

print("Iteration #3.1.16")
source("./src/CDBcleanup_fullpar_addr_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Seventeenth set of 2 chunks
basedatawrt12 = basedatawrtn[start33:end33,]
basedatawrt22 = basedatawrtn[start34:end34,]

print("Iteration #3.1.17")
source("./src/CDBcleanup_fullpar_addr_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Eighteenth set of 2 chunks
basedatawrt12 = basedatawrtn[start35:end35,]
basedatawrt22 = basedatawrtn[start36:end36,]

print("Iteration #3.1.18")
source("./src/CDBcleanup_fullpar_addr_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Nineteenth set of 2 chunks
basedatawrt12 = basedatawrtn[start37:end37,]
basedatawrt22 = basedatawrtn[start38:end38,]

print("Iteration #3.1.19")
source("./src/CDBcleanup_fullpar_addr_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Twentieth set of 2 chunks
basedatawrt12 = basedatawrtn[start39:end39,]
basedatawrt22 = basedatawrtn[start40:end40,]

print("Iteration #3.1.20")
source("./src/CDBcleanup_fullpar_addr_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#write.csv(outbdwrtn, "outbdwrtn3-1.csv")


#Iteration #3.2 - Process the resulting dataframe from iteration #3.1 (sorted by Reverse Address)
#Iteration #3.2 - Process the resulting dataframe from iteration #3.1 (sorted by Reverse Address)
#Iteration #3.2 - Process the resulting dataframe from iteration #3.1 (sorted by Reverse Address)

basedatawrtn <- outbdwrtn

#Reverse the Address
Addr <- basedatawrtn$ADDRESS_new
RevAddr <- sapply(Addr, function(x) { paste(rev(substring(x,1:nchar(x),1:nchar(x))),collapse="") })
names(RevAddr) <- NULL
basedatawrtn$ADDRESS_new <- RevAddr

basedatawrtn <- basedatawrtn[order(basedatawrtn$ADDRESS_new,-basedatawrtn$CUSTOMER_NUMBER),]

outbdwrtn <- NULL

#Process First set of 2 chunks
basedatawrt12 = basedatawrtn[start1:end1,]
basedatawrt22 = basedatawrtn[start2:end2,]

print("Iteration #3.2.1")
source("./src/CDBcleanup_fullpar_addr_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Second set of 2 chunks
basedatawrt12 = basedatawrtn[start3:end3,]
basedatawrt22 = basedatawrtn[start4:end4,]

print("Iteration #3.2.2")
source("./src/CDBcleanup_fullpar_addr_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Third set of 2 chunks
basedatawrt12 = basedatawrtn[start5:end5,]
basedatawrt22 = basedatawrtn[start6:end6,]

print("Iteration #3.2.3")
source("./src/CDBcleanup_fullpar_addr_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Fourth set of 2 chunks
basedatawrt12 = basedatawrtn[start7:end7,]
basedatawrt22 = basedatawrtn[start8:end8,]

print("Iteration #3.2.4")
source("./src/CDBcleanup_fullpar_addr_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Fifth set of 2 chunks
basedatawrt12 = basedatawrtn[start9:end9,]
basedatawrt22 = basedatawrtn[start10:end10,]

print("Iteration #3.2.5")
source("./src/CDBcleanup_fullpar_addr_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Sixth set of 2 chunks
basedatawrt12 = basedatawrtn[start11:end11,]
basedatawrt22 = basedatawrtn[start12:end12,]

print("Iteration #3.2.6")
source("./src/CDBcleanup_fullpar_addr_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Seventh set of 2 chunks
basedatawrt12 = basedatawrtn[start13:end13,]
basedatawrt22 = basedatawrtn[start14:end14,]

print("Iteration #3.2.7")
source("./src/CDBcleanup_fullpar_addr_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Eigth set of 2 chunks
basedatawrt12 = basedatawrtn[start15:end15,]
basedatawrt22 = basedatawrtn[start16:end16,]

print("Iteration #3.2.8")
source("./src/CDBcleanup_fullpar_addr_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Ninth set of 2 chunks
basedatawrt12 = basedatawrtn[start17:end17,]
basedatawrt22 = basedatawrtn[start18:end18,]

print("Iteration #3.2.9")
source("./src/CDBcleanup_fullpar_addr_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Tenth set of 2 chunks
basedatawrt12 = basedatawrtn[start19:end19,]
basedatawrt22 = basedatawrtn[start20:end20,]

print("Iteration #3.2.10")
source("./src/CDBcleanup_fullpar_addr_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Eleventh set of 2 chunks
basedatawrt12 = basedatawrtn[start21:end21,]
basedatawrt22 = basedatawrtn[start22:end22,]

print("Iteration #3.2.11")
source("./src/CDBcleanup_fullpar_addr_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Twelveth set of 2 chunks
basedatawrt12 = basedatawrtn[start23:end23,]
basedatawrt22 = basedatawrtn[start24:end24,]

print("Iteration #3.2.12")
source("./src/CDBcleanup_fullpar_addr_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Thirteenth set of 2 chunks
basedatawrt12 = basedatawrtn[start25:end25,]
basedatawrt22 = basedatawrtn[start26:end26,]

print("Iteration #3.2.13")
source("./src/CDBcleanup_fullpar_addr_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Fourteenth set of 2 chunks
basedatawrt12 = basedatawrtn[start27:end27,]
basedatawrt22 = basedatawrtn[start28:end28,]

print("Iteration #3.2.14")
source("./src/CDBcleanup_fullpar_addr_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Fifteenth set of 2 chunks
basedatawrt12 = basedatawrtn[start29:end29,]
basedatawrt22 = basedatawrtn[start30:end30,]

print("Iteration #3.2.15")
source("./src/CDBcleanup_fullpar_addr_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Sixteenth set of 2 chunks
basedatawrt12 = basedatawrtn[start31:end31,]
basedatawrt22 = basedatawrtn[start32:end32,]

print("Iteration #3.2.16")
source("./src/CDBcleanup_fullpar_addr_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Seventeenth set of 2 chunks
basedatawrt12 = basedatawrtn[start33:end33,]
basedatawrt22 = basedatawrtn[start34:end34,]

print("Iteration #3.2.17")
source("./src/CDBcleanup_fullpar_addr_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Eighteenth set of 2 chunks
basedatawrt12 = basedatawrtn[start35:end35,]
basedatawrt22 = basedatawrtn[start36:end36,]

print("Iteration #3.2.18")
source("./src/CDBcleanup_fullpar_addr_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Nineteenth set of 2 chunks
basedatawrt12 = basedatawrtn[start37:end37,]
basedatawrt22 = basedatawrtn[start38:end38,]

print("Iteration #3.2.19")
source("./src/CDBcleanup_fullpar_addr_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Process Twentieth set of 2 chunks
basedatawrt12 = basedatawrtn[start39:end39,]
basedatawrt22 = basedatawrtn[start40:end40,]

print("Iteration #3.2.20")
source("./src/CDBcleanup_fullpar_addr_match_upd.R")

#Concatenate updated set of 2 chunks into output dataframe
outbdwrtn <- rbind(outbdwrtn, datbdwrtn)

#Reverse the reversed Address to correct format
Addr <- outbdwrtn$ADDRESS_new
RevAddr <- sapply(Addr, function(x) { paste(rev(substring(x,1:nchar(x),1:nchar(x))),collapse="") })
names(RevAddr) <- NULL
outbdwrtn$ADDRESS_new <- RevAddr

#write.csv(outbdwrtn, "outbdwrtn3_2.csv")


#Link Masterid of related customers to Maximum masterid
#Link Masterid of related customers to Maximum masterid

basedatawrtn <- outbdwrtn
basedatawrtn <- basedatawrtn[order(-basedatawrtn$CUSTOMER_NUMBER),]

basedatawrt1 <- basedatawrtn[,c("CUSTOMER_NUMBER","masteridnew","description")]

basedatawrtn <- NULL
basedatawrtn <- cbind(basedatawrt1, basedatarem1)

outbdwrtn <- NULL

#First set of regional chunks
basedatawrt12 <- subset(basedatawrtn, REGION=="Region1")
basedatawrt22 <- subset(basedatawrtn, REGION=="Region2")

bdwlist <- NULL
bdwlist <- c(basedatawrt12, basedatawrt22)

#Parallel process of 2 chunks (calling funtion which does Phone Match tagging)
res <- foreach(i=1:2) %dopar% {
ParFullFn_linkmid(data.frame(bdwlist[(((i-1)*6)+1):(i*6)], stringsAsFactors=FALSE))
}   

#Update revised Masterid
bdwpout1 <- res[[1]]
names(bdwpout1) <- NULL
basedatawrt12$masteridnew <- bdwpout1
bdwpout2 <- res[[2]]
names(bdwpout2) <- NULL
basedatawrt22$masteridnew <- bdwpout2

outbdwrtn <- rbind(outbdwrtn, basedatawrt12, basedatawrt22)

#gargbage cleanup
rm(res)
rm(bdwpout1)
rm(bdwpout2)
rm(bdwlist)
rm(basedatawrt12)
rm(basedatawrt22)

#Second set of regional chunks
basedatawrt12 <- subset(basedatawrtn, REGION=="Region3")
basedatawrt22 <- subset(basedatawrtn, REGION=="Region4")

bdwlist <- NULL
bdwlist <- c(basedatawrt12, basedatawrt22)

#Parallel process of 2 chunks (calling funtion which does Phone Match tagging)
res <- foreach(i=1:2) %dopar% {
ParFullFn_linkmid(data.frame(bdwlist[(((i-1)*6)+1):(i*6)], stringsAsFactors=FALSE))
}   

#Update revised Masterid
bdwpout1 <- res[[1]]
names(bdwpout1) <- NULL
basedatawrt12$masteridnew <- bdwpout1
bdwpout2 <- res[[2]]
names(bdwpout2) <- NULL
basedatawrt22$masteridnew <- bdwpout2

outbdwrtn <- rbind(outbdwrtn, basedatawrt12, basedatawrt22)

#gargbage cleanup
rm(res)
rm(bdwpout1)
rm(bdwpout2)
rm(bdwlist)
rm(basedatawrt12)
rm(basedatawrt22)

#Third set of regional chunks
basedatawrt12 <- subset(basedatawrtn, REGION=="Region5")
basedatawrt22 <- subset(basedatawrtn, REGION=="Region6")

bdwlist <- NULL
bdwlist <- c(basedatawrt12, basedatawrt22)

#Parallel process of 2 chunks (calling funtion which does Phone Match tagging)
res <- foreach(i=1:2) %dopar% {
ParFullFn_linkmid(data.frame(bdwlist[(((i-1)*6)+1):(i*6)], stringsAsFactors=FALSE))
}   

#Update revised Masterid
bdwpout1 <- res[[1]]
names(bdwpout1) <- NULL
basedatawrt12$masteridnew <- bdwpout1
bdwpout2 <- res[[2]]
names(bdwpout2) <- NULL
basedatawrt22$masteridnew <- bdwpout2

outbdwrtn <- rbind(outbdwrtn, basedatawrt12, basedatawrt22)

#gargbage cleanup
rm(res)
rm(bdwpout1)
rm(bdwpout2)
rm(bdwlist)
rm(basedatawrt12)
rm(basedatawrt22)

#Fourth set of regional chunks
basedatawrt12 <- subset(basedatawrtn, REGION=="Region7")
basedatawrt22 <- subset(basedatawrtn, REGION=="Region8")

bdwlist <- NULL
bdwlist <- c(basedatawrt12, basedatawrt22)

#Parallel process of 2 chunks (calling funtion which does Phone Match tagging)
res <- foreach(i=1:2) %dopar% {
ParFullFn_linkmid(data.frame(bdwlist[(((i-1)*6)+1):(i*6)], stringsAsFactors=FALSE))
}   

#Update revised Masterid
bdwpout1 <- res[[1]]
names(bdwpout1) <- NULL
basedatawrt12$masteridnew <- bdwpout1
bdwpout2 <- res[[2]]
names(bdwpout2) <- NULL
basedatawrt22$masteridnew <- bdwpout2

outbdwrtn <- rbind(outbdwrtn, basedatawrt12, basedatawrt22)

#gargbage cleanup
rm(res)
rm(bdwpout1)
rm(bdwpout2)
rm(bdwlist)
rm(basedatawrt12)
rm(basedatawrt22)

#Fifth set of regional chunks
basedatawrt12 <- subset(basedatawrtn, REGION=="Region9")
basedatawrt22 <- subset(basedatawrtn, REGION=="Region10")

bdwlist <- NULL
bdwlist <- c(basedatawrt12, basedatawrt22)

#Parallel process of 2 chunks (calling funtion which does Phone Match tagging)
res <- foreach(i=1:2) %dopar% {
ParFullFn_linkmid(data.frame(bdwlist[(((i-1)*6)+1):(i*6)], stringsAsFactors=FALSE))
}   

#Update revised Masterid
bdwpout1 <- res[[1]]
names(bdwpout1) <- NULL
basedatawrt12$masteridnew <- bdwpout1
bdwpout2 <- res[[2]]
names(bdwpout2) <- NULL
basedatawrt22$masteridnew <- bdwpout2

outbdwrtn <- rbind(outbdwrtn, basedatawrt12, basedatawrt22)

#gargbage cleanup
rm(res)
rm(bdwpout1)
rm(bdwpout2)
rm(bdwlist)
rm(basedatawrt12)
rm(basedatawrt22)

#Sixth set of regional chunks
basedatawrt12 <- subset(basedatawrtn, REGION=="Region11")
basedatawrt22 <- subset(basedatawrtn, REGION=="Region12")

bdwlist <- NULL
bdwlist <- c(basedatawrt12, basedatawrt22)

#Parallel process of 2 chunks (calling funtion which does Phone Match tagging)
res <- foreach(i=1:2) %dopar% {
ParFullFn_linkmid(data.frame(bdwlist[(((i-1)*6)+1):(i*6)], stringsAsFactors=FALSE))
}   

#Update revised Masterid
bdwpout1 <- res[[1]]
names(bdwpout1) <- NULL
basedatawrt12$masteridnew <- bdwpout1
bdwpout2 <- res[[2]]
names(bdwpout2) <- NULL
basedatawrt22$masteridnew <- bdwpout2

outbdwrtn <- rbind(outbdwrtn, basedatawrt12, basedatawrt22)

#gargbage cleanup
rm(res)
rm(bdwpout1)
rm(bdwpout2)
rm(bdwlist)
rm(basedatawrt12)
rm(basedatawrt22)

#Seventh set of regional chunks
basedatawrt12 <- subset(basedatawrtn, REGION=="Region13")
basedatawrt22 <- subset(basedatawrtn, REGION=="Region14")

bdwlist <- NULL
bdwlist <- c(basedatawrt12, basedatawrt22)

#Parallel process of 2 chunks (calling funtion which does Phone Match tagging)
res <- foreach(i=1:2) %dopar% {
ParFullFn_linkmid(data.frame(bdwlist[(((i-1)*6)+1):(i*6)], stringsAsFactors=FALSE))
}   

#Update revised Masterid
bdwpout1 <- res[[1]]
names(bdwpout1) <- NULL
basedatawrt12$masteridnew <- bdwpout1
bdwpout2 <- res[[2]]
names(bdwpout2) <- NULL
basedatawrt22$masteridnew <- bdwpout2

outbdwrtn <- rbind(outbdwrtn, basedatawrt12, basedatawrt22)

#gargbage cleanup
rm(res)
rm(bdwpout1)
rm(bdwpout2)
rm(bdwlist)
rm(basedatawrt12)
rm(basedatawrt22)

#Eigth set of regional chunks
basedatawrt12 <- subset(basedatawrtn, REGION=="Region15")
basedatawrt22 <- subset(basedatawrtn, REGION=="Region16")

bdwlist <- NULL
bdwlist <- c(basedatawrt12, basedatawrt22)

#Parallel process of 2 chunks (calling funtion which does Phone Match tagging)
res <- foreach(i=1:2) %dopar% {
ParFullFn_linkmid(data.frame(bdwlist[(((i-1)*6)+1):(i*6)], stringsAsFactors=FALSE))
}   

#Update revised Masterid
bdwpout1 <- res[[1]]
names(bdwpout1) <- NULL
basedatawrt12$masteridnew <- bdwpout1
bdwpout2 <- res[[2]]
names(bdwpout2) <- NULL
basedatawrt22$masteridnew <- bdwpout2

outbdwrtn <- rbind(outbdwrtn, basedatawrt12, basedatawrt22)

#gargbage cleanup
rm(res)
rm(bdwpout1)
rm(bdwpout2)
rm(bdwlist)
rm(basedatawrt12)
rm(basedatawrt22)

basedatawrtn <- outbdwrtn
basedatawrtn <- basedatawrtn[order(-basedatawrtn$CUSTOMER_NUMBER),]

#Recombine dataframes and write to output file
basedatarev <- cbind(basedatawrtn, basedatarem2, basedatarem3)

ofileval = c("CDBOutputiteration", sys_date)
tochg <- c("[[:punct:]]","[[:space:]]")
tochgdlm <- paste0(tochg, collapse="|")
ofilename <- gsub(tochgdlm,"", ofileval)
ofilename <- c(ofilename, "csv")
ofilename = paste0(ofilename, collapse=".")
opathval = c(cwd,"output", ofilename)
opathfile = paste0(opathval, collapse="/")
write.csv(basedatarev, opathfile)

print("Execution completed")
print("-------------------")

###### End of Script ######