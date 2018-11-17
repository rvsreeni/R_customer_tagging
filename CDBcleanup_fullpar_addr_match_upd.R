#Check boundaries of chunks to see if records (with same Address) spanned across 2 chunks, and if so, move them to 1 chunk
if (exists("ParFn_addr_bndchk", mode="function"))
   source("./src/CDBcleanup_par_bndchk_func.R")

bdwlist <- NULL
bdwlist <- c(basedatawrt12, basedatawrt22)

#Parallel process of 2 chunks (calling funtion which does Phone Match tagging)
res <- foreach(i=1:2, .packages="stringdist") %dopar% {
ParFn_addr_match(data.frame(bdwlist[(((i-1)*6)+1):(i*6)], stringsAsFactors=FALSE))
}   

#Update revised Masterid, Description (returned by ParFn_addr_match)

#Get updated/revised masterid, description (returned by ParFn_addr_match)
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
lenbdwrt  <- nrow(datbdwrtn)

#Get existing/current masterid
Mastid <- data.frame(datbdwrtn$masteridnew)
colnames(Mastid) <- c("Mid")
Mastid$seq <- 1:nrow(Mastid)

## Compare masterid (current vs revised) - if there is a change, for all the customers (tagged to current Masterid) change to revised Masterid
for(i in (1:(lenbdwrt))) {
z <- match(Mastid$Mid[i], New_Masterid[i], nomatch=0)
if (!(z > 0)) {
kseq <- Mastid[(Mastid$Mid==Mastid$Mid[i]),]$seq 
midv <- NULL
for (k in kseq) { midv <- c(midv, New_Masterid[k]) }
maxid <- max(midv)
for (k in kseq) { New_Masterid[k] = maxid  }
}}

#Update revised masterid, description in dataframe
datbdwrtn$masteridnew <- New_Masterid
datbdwrtn$description <- Description

#gargbage cleanup
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