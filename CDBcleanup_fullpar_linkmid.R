#This function takes in dataframe as input
#Returns Masterid vector (related customers linked to same Masterid) 

ParFullFn_linkmid <- function(dataexm)
{
Masteridnew <- dataexm$masteridnew
uniqmid <- unique(dataexm$masteridnew)
lenuqmid <- length(uniqmid)
print(lenuqmid)
for (mid in uniqmid) {
tempcid <- subset(dataexm, CUSTOMER_NUMBER == mid)
if (nrow(tempcid) > 0) {
maxid <- max(tempcid$masteridnew)
ind <-  which(Masteridnew == mid)
for (i in ind) { Masteridnew[i] <- maxid }
}
rm(tempcid)
}  
dataexm$masteridnew <- Masteridnew
return(dataexm[,2])
}
