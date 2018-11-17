#Load Phone boundary check code
#Check boundaries of chunks to see if records (with same Phone#) spanned across 2 chunks, and if so, move them to 1 chunk
if (exists("ParFn_phone_bndchk", mode="function"))
   source("./src/CDBcleanup_par_bndchk_func.R")
   
bdwlist <- NULL
bdwlist <- c(basedatawrt12, basedatawrt22)

#Parallel process of 2 chunks (calling funtion which does Phone Match tagging)
res <- foreach(i=1:2, .packages="stringdist") %dopar% {
ParFn_phone_match(data.frame(bdwlist[(((i-1)*6)+1):(i*6)], stringsAsFactors=FALSE))
}   

#Update revised Masterid, Description (returned by ParFn_phone_match)

#Get updated/revised masterid, description (returned by ParFn_phone_match)
New_Masterid <- NULL
Decription <- NULL
tmp <- NULL
tmp <- data.frame(res[1], stringsAsFactors=FALSE)
mstid1 <- tmp$masteridnew
desc1 <- tmp$description
tmp <- NULL
tmp <- data.frame(res[2], stringsAsFactors=FALSE)
mstid2 <- tmp$masteridnew
desc2 <- tmp$description
New_Masterid <- c(mstid1, mstid2)
Description <- c(desc1, desc2)

datbdwrtn <- rbind(basedatawrt12, basedatawrt22)

#Update revised masterid, description in dataframe
datbdwrtn$masteridnew <- New_Masterid
datbdwrtn$description <- Description

#garbage cleanup
rm(res)
rm(tmp)
rm(bdwlist)
rm(basedatawrt12)
rm(basedatawrt22)
rm(mstid1)
rm(mstid2)
rm(desc1)
rm(desc2)
rm(New_Masterid)
rm(Description)