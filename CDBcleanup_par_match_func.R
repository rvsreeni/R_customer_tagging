#This script has the Match tagging functions(Phone, Name, Address)

#This function takes in as input dataframe (sorted by Phone)
#Returns updated Masterid, Description vectors (based on Match tagging logic)

ParFn_phone_match <- function(dataexm)
{
lendataexm <- nrow(dataexm)
Phone   <- dataexm$PHONE_NUMBER_new
Name    <- dataexm$CUSTOMER_NAME_new
Address <- dataexm$ADDRESS_new
masteridnew <- dataexm$masteridnew
description <- dataexm$description
for(i in (1:(lendataexm-1))) {
  print(i)
  j <- i+1
  b <- match(Phone[i], Phone[j], nomatch=0)
## Perform inner loop (if there is Exact match on Phone) 
  while ((b > 0) & (j <= lendataexm)) {

    b <- match(Phone[i],Phone[j],nomatch = 0)
    c <- match(Name[i],Name[j],nomatch = 0)
    d <- match(Address[i],Address[j],nomatch = 0)
    
    if(b>0){
      if(d>0){
        if(c>0){
          Mastid  <- NULL
	  Mastid  <- data.frame(masteridnew)
	  colnames(Mastid) <- c("Mid")
	  Mastid$seq <- 1:nrow(Mastid)
	  maxid = max(masteridnew[i],masteridnew[j])
	  kseq <- Mastid[(Mastid$Mid==masteridnew[i]),]$seq 
	  for (k in kseq) { masteridnew[k]= maxid }
	  kseq <- Mastid[(Mastid$Mid==masteridnew[j]),]$seq 
	  for (k in kseq) { masteridnew[k]= maxid }
	  masteridnew[i]= maxid
	  masteridnew[j]= maxid
         description[i]="Master"
         description[j]="Same customer"
        }
        else if(amatch(Name[i],Name[j], method="dl", maxDist=2, nomatch=0)[[1]] > 0){
          Mastid  <- NULL
 	  Mastid  <- data.frame(masteridnew)
	  colnames(Mastid) <- c("Mid")
	  Mastid$seq <- 1:nrow(Mastid)
	  maxid = max(masteridnew[i],masteridnew[j])
	  kseq <- Mastid[(Mastid$Mid==masteridnew[i]),]$seq 
	  for (k in kseq) { masteridnew[k]= maxid }
	  kseq <- Mastid[(Mastid$Mid==masteridnew[j]),]$seq 
	  for (k in kseq) { masteridnew[k]= maxid }
	  masteridnew[i]= maxid
	  masteridnew[j]= maxid
         description[i]="Master"
         description[j]="Same customer"
        }
        else{
           Mastid  <- NULL
 	   Mastid  <- data.frame(masteridnew)
	   colnames(Mastid) <- c("Mid")
	   Mastid$seq <- 1:nrow(Mastid)
	   maxid = max(masteridnew[i],masteridnew[j])
	   kseq <- Mastid[(Mastid$Mid==masteridnew[i]),]$seq 
	   for (k in kseq) { masteridnew[k]= maxid }
	   kseq <- Mastid[(Mastid$Mid==masteridnew[j]),]$seq 
	   for (k in kseq) { masteridnew[k]= maxid }
	   masteridnew[i]= maxid
	   masteridnew[j]= maxid
          description[i]="Master"
          description[j]="Relation"
        }
      }
      else if(c>0){
        if(amatch(Address[i],Address[j], method="dl", maxDist=10, nomatch=0)[[1]] > 0){
           Mastid  <- NULL
	   Mastid  <- data.frame(masteridnew)
	   colnames(Mastid) <- c("Mid")
	   Mastid$seq <- 1:nrow(Mastid)
	   maxid = max(masteridnew[i],masteridnew[j])
	   kseq <- Mastid[(Mastid$Mid==masteridnew[i]),]$seq 
	   for (k in kseq) { masteridnew[k]= maxid }
	   kseq <- Mastid[(Mastid$Mid==masteridnew[j]),]$seq 
	   for (k in kseq) { masteridnew[k]= maxid }
	   masteridnew[i]= maxid
	   masteridnew[j]= maxid
          description[i]="Master"
          description[j]="Same customer"
        }
        else{
           Mastid  <- NULL
 	   Mastid  <- data.frame(masteridnew)
	   colnames(Mastid) <- c("Mid")
	   Mastid$seq <- 1:nrow(Mastid)
	   maxid = max(masteridnew[i],masteridnew[j])
	   kseq <- Mastid[(Mastid$Mid==masteridnew[i]),]$seq 
	   for (k in kseq) { masteridnew[k]= maxid }
	   kseq <- Mastid[(Mastid$Mid==masteridnew[j]),]$seq 
	   for (k in kseq) { masteridnew[k]= maxid }
	   masteridnew[i]= maxid
	   masteridnew[j]= maxid
          description[i]="Master"
          description[j]="Name and Phone number same"
        }
      }
      else if(amatch(Address[i],Address[j], method="dl", maxDist=10, nomatch=0)[[1]] > 0){
         Mastid  <- NULL
 	 Mastid  <- data.frame(masteridnew)
	 colnames(Mastid) <- c("Mid")
	 Mastid$seq <- 1:nrow(Mastid)
	 maxid = max(masteridnew[i],masteridnew[j])
	 kseq <- Mastid[(Mastid$Mid==masteridnew[i]),]$seq 
	 for (k in kseq) { masteridnew[k]= maxid }
	 kseq <- Mastid[(Mastid$Mid==masteridnew[j]),]$seq 
	 for (k in kseq) { masteridnew[k]= maxid }
	 masteridnew[i]= maxid
	 masteridnew[j]= maxid
        description[i]="Master"
        description[j]="Relation"
      }
      else if(amatch(Name[i],Name[j], method="dl", maxDist=2, nomatch=0)[[1]] > 0){
        if(amatch(Address[i],Address[j], method="dl", maxDist=10, nomatch=0)[[1]] > 0){
           Mastid  <- NULL
	   Mastid  <- data.frame(masteridnew)
	   colnames(Mastid) <- c("Mid")
	   Mastid$seq <- 1:nrow(Mastid)
	   maxid = max(masteridnew[i],masteridnew[j])
	   kseq <- Mastid[(Mastid$Mid==masteridnew[i]),]$seq 
	   for (k in kseq) { masteridnew[k]= maxid }
	   kseq <- Mastid[(Mastid$Mid==masteridnew[j]),]$seq 
	   for (k in kseq) { masteridnew[k]= maxid }
	   masteridnew[i]= maxid
	   masteridnew[j]= maxid
          description[i]="Master"
          description[j]="Same customer"
        }
        else{  
           Mastid  <- NULL
	   Mastid  <- data.frame(masteridnew)
	   colnames(Mastid) <- c("Mid")
	   Mastid$seq <- 1:nrow(Mastid)
	   maxid = max(masteridnew[i],masteridnew[j])
	   kseq <- Mastid[(Mastid$Mid==masteridnew[i]),]$seq 
	   for (k in kseq) { masteridnew[k]= maxid }
	   kseq <- Mastid[(Mastid$Mid==masteridnew[j]),]$seq 
	   for (k in kseq) { masteridnew[k]= maxid }
	   masteridnew[i]= maxid
	   masteridnew[j]= maxid
          description[i]="Master"
          description[j]="Name and Phone number same"
        }
      }
    }
    else if(c>0){
      if(d>0){
         Mastid  <- NULL
	 Mastid  <- data.frame(masteridnew)
	 colnames(Mastid) <- c("Mid")
	 Mastid$seq <- 1:nrow(Mastid)
	 maxid = max(masteridnew[i],masteridnew[j])
	 kseq <- Mastid[(Mastid$Mid==masteridnew[i]),]$seq 
	 for (k in kseq) { masteridnew[k]= maxid }
	 kseq <- Mastid[(Mastid$Mid==masteridnew[j]),]$seq 
	 for (k in kseq) { masteridnew[k]= maxid }
	 masteridnew[i]= maxid
	 masteridnew[j]= maxid
        description[i]="Master"
        description[j]="Name and Address same"
      }
      else if(amatch(Address[i],Address[j], method="dl", maxDist=10, nomatch=0)[[1]] > 0){
         Mastid  <- NULL
	 Mastid  <- data.frame(masteridnew)
	 colnames(Mastid) <- c("Mid")
	 Mastid$seq <- 1:nrow(Mastid)
	 maxid = max(masteridnew[i],masteridnew[j])
	 kseq <- Mastid[(Mastid$Mid==masteridnew[i]),]$seq 
	 for (k in kseq) { masteridnew[k]= maxid }
	 kseq <- Mastid[(Mastid$Mid==masteridnew[j]),]$seq 
	 for (k in kseq) { masteridnew[k]= maxid }
	 masteridnew[i]= maxid
	 masteridnew[j]= maxid
        description[i]="Master"
        description[j]="Name and Address same"
      }
    }
    else if(d>0){
      if(amatch(Name[i],Name[j], method="dl", maxDist=2, nomatch=0)[[1]] > 0){
         Mastid  <- NULL
	 Mastid  <- data.frame(masteridnew)
	 colnames(Mastid) <- c("Mid")
	 Mastid$seq <- 1:nrow(Mastid)
	 maxid = max(masteridnew[i],masteridnew[j])
	 kseq <- Mastid[(Mastid$Mid==masteridnew[i]),]$seq 
	 for (k in kseq) { masteridnew[k]= maxid }
	 kseq <- Mastid[(Mastid$Mid==masteridnew[j]),]$seq 
	 for (k in kseq) { masteridnew[k]= maxid }
	 masteridnew[i]= maxid
	 masteridnew[j]= maxid
        description[i]="Master"
        description[j]="Name and Address same"
      }
    }

    j <- j+1
    b <- match(Phone[i], Phone[j], nomatch=0)
  }
}
dataexm$masteridnew <- masteridnew
dataexm$description <- description
return(dataexm[,5:6])
}



#This function takes in as input dataframe (sorted by Name)
#Returns updated Masterid, Description vectors (based on Match tagging logic)

ParFn_name_match <- function(dataexm)
{
lendataexm <- nrow(dataexm)
Phone   <- dataexm$PHONE_NUMBER_new
Name    <- dataexm$CUSTOMER_NAME_new
Address <- dataexm$ADDRESS_new
masteridnew <- dataexm$masteridnew
description <- dataexm$description
for(i in (1:(lendataexm-1))) {
  print(i)
  j <- i+1
  c <- match(Name[i], Name[j], nomatch=0)
  if (c == 0) { c <- amatch(Name[i], Name[j], method="dl", maxDist=2, nomatch=0) }
## Perform inner loop (if there is either Exact or Approximate match on Name) 
  while ((c > 0) & (j <= lendataexm)) {

    b <- match(Phone[i],Phone[j],nomatch = 0)
    c <- match(Name[i],Name[j],nomatch = 0)
    d <- match(Address[i],Address[j],nomatch = 0)
    
    if(b>0){
      if(d>0){
        if(c>0){
          Mastid  <- NULL
	  Mastid  <- data.frame(masteridnew)
	  colnames(Mastid) <- c("Mid")
	  Mastid$seq <- 1:nrow(Mastid)
	  maxid = max(masteridnew[i],masteridnew[j])
	  kseq <- Mastid[(Mastid$Mid==masteridnew[i]),]$seq 
	  for (k in kseq) { masteridnew[k]= maxid }
	  kseq <- Mastid[(Mastid$Mid==masteridnew[j]),]$seq 
	  for (k in kseq) { masteridnew[k]= maxid }
	  masteridnew[i]= maxid
	  masteridnew[j]= maxid
         description[i]="Master"
         description[j]="Same customer"
        }
        else if(amatch(Name[i],Name[j], method="dl", maxDist=2, nomatch=0)[[1]] > 0){
          Mastid  <- NULL
 	  Mastid  <- data.frame(masteridnew)
	  colnames(Mastid) <- c("Mid")
	  Mastid$seq <- 1:nrow(Mastid)
	  maxid = max(masteridnew[i],masteridnew[j])
	  kseq <- Mastid[(Mastid$Mid==masteridnew[i]),]$seq 
	  for (k in kseq) { masteridnew[k]= maxid }
	  kseq <- Mastid[(Mastid$Mid==masteridnew[j]),]$seq 
	  for (k in kseq) { masteridnew[k]= maxid }
	  masteridnew[i]= maxid
	  masteridnew[j]= maxid
         description[i]="Master"
         description[j]="Same customer"
        }
        else{
           Mastid  <- NULL
 	   Mastid  <- data.frame(masteridnew)
	   colnames(Mastid) <- c("Mid")
	   Mastid$seq <- 1:nrow(Mastid)
	   maxid = max(masteridnew[i],masteridnew[j])
	   kseq <- Mastid[(Mastid$Mid==masteridnew[i]),]$seq 
	   for (k in kseq) { masteridnew[k]= maxid }
	   kseq <- Mastid[(Mastid$Mid==masteridnew[j]),]$seq 
	   for (k in kseq) { masteridnew[k]= maxid }
	   masteridnew[i]= maxid
	   masteridnew[j]= maxid
          description[i]="Master"
          description[j]="Relation"
        }
      }
      else if(c>0){
        if(amatch(Address[i],Address[j], method="dl", maxDist=10, nomatch=0)[[1]] > 0){
           Mastid  <- NULL
	   Mastid  <- data.frame(masteridnew)
	   colnames(Mastid) <- c("Mid")
	   Mastid$seq <- 1:nrow(Mastid)
	   maxid = max(masteridnew[i],masteridnew[j])
	   kseq <- Mastid[(Mastid$Mid==masteridnew[i]),]$seq 
	   for (k in kseq) { masteridnew[k]= maxid }
	   kseq <- Mastid[(Mastid$Mid==masteridnew[j]),]$seq 
	   for (k in kseq) { masteridnew[k]= maxid }
	   masteridnew[i]= maxid
	   masteridnew[j]= maxid
          description[i]="Master"
          description[j]="Same customer"
        }
        else{
           Mastid  <- NULL
 	   Mastid  <- data.frame(masteridnew)
	   colnames(Mastid) <- c("Mid")
	   Mastid$seq <- 1:nrow(Mastid)
	   maxid = max(masteridnew[i],masteridnew[j])
	   kseq <- Mastid[(Mastid$Mid==masteridnew[i]),]$seq 
	   for (k in kseq) { masteridnew[k]= maxid }
	   kseq <- Mastid[(Mastid$Mid==masteridnew[j]),]$seq 
	   for (k in kseq) { masteridnew[k]= maxid }
	   masteridnew[i]= maxid
	   masteridnew[j]= maxid
          description[i]="Master"
          description[j]="Name and Phone number same"
        }
      }
      else if(amatch(Address[i],Address[j], method="dl", maxDist=10, nomatch=0)[[1]] > 0){
         Mastid  <- NULL
 	 Mastid  <- data.frame(masteridnew)
	 colnames(Mastid) <- c("Mid")
	 Mastid$seq <- 1:nrow(Mastid)
	 maxid = max(masteridnew[i],masteridnew[j])
	 kseq <- Mastid[(Mastid$Mid==masteridnew[i]),]$seq 
	 for (k in kseq) { masteridnew[k]= maxid }
	 kseq <- Mastid[(Mastid$Mid==masteridnew[j]),]$seq 
	 for (k in kseq) { masteridnew[k]= maxid }
	 masteridnew[i]= maxid
	 masteridnew[j]= maxid
        description[i]="Master"
        description[j]="Relation"
      }
      else if(amatch(Name[i],Name[j], method="dl", maxDist=2, nomatch=0)[[1]] > 0){
        if(amatch(Address[i],Address[j], method="dl", maxDist=10, nomatch=0)[[1]] > 0){
           Mastid  <- NULL
	   Mastid  <- data.frame(masteridnew)
	   colnames(Mastid) <- c("Mid")
	   Mastid$seq <- 1:nrow(Mastid)
	   maxid = max(masteridnew[i],masteridnew[j])
	   kseq <- Mastid[(Mastid$Mid==masteridnew[i]),]$seq 
	   for (k in kseq) { masteridnew[k]= maxid }
	   kseq <- Mastid[(Mastid$Mid==masteridnew[j]),]$seq 
	   for (k in kseq) { masteridnew[k]= maxid }
	   masteridnew[i]= maxid
	   masteridnew[j]= maxid
          description[i]="Master"
          description[j]="Same customer"
        }
        else{  
           Mastid  <- NULL
	   Mastid  <- data.frame(masteridnew)
	   colnames(Mastid) <- c("Mid")
	   Mastid$seq <- 1:nrow(Mastid)
	   maxid = max(masteridnew[i],masteridnew[j])
	   kseq <- Mastid[(Mastid$Mid==masteridnew[i]),]$seq 
	   for (k in kseq) { masteridnew[k]= maxid }
	   kseq <- Mastid[(Mastid$Mid==masteridnew[j]),]$seq 
	   for (k in kseq) { masteridnew[k]= maxid }
	   masteridnew[i]= maxid
	   masteridnew[j]= maxid
          description[i]="Master"
          description[j]="Name and Phone number same"
        }
      }
    }
    else if(c>0){
      if(d>0){
         Mastid  <- NULL
	 Mastid  <- data.frame(masteridnew)
	 colnames(Mastid) <- c("Mid")
	 Mastid$seq <- 1:nrow(Mastid)
	 maxid = max(masteridnew[i],masteridnew[j])
	 kseq <- Mastid[(Mastid$Mid==masteridnew[i]),]$seq 
	 for (k in kseq) { masteridnew[k]= maxid }
	 kseq <- Mastid[(Mastid$Mid==masteridnew[j]),]$seq 
	 for (k in kseq) { masteridnew[k]= maxid }
	 masteridnew[i]= maxid
	 masteridnew[j]= maxid
        description[i]="Master"
        description[j]="Name and Address same"
      }
      else if(amatch(Address[i],Address[j], method="dl", maxDist=10, nomatch=0)[[1]] > 0){
         Mastid  <- NULL
	 Mastid  <- data.frame(masteridnew)
	 colnames(Mastid) <- c("Mid")
	 Mastid$seq <- 1:nrow(Mastid)
	 maxid = max(masteridnew[i],masteridnew[j])
	 kseq <- Mastid[(Mastid$Mid==masteridnew[i]),]$seq 
	 for (k in kseq) { masteridnew[k]= maxid }
	 kseq <- Mastid[(Mastid$Mid==masteridnew[j]),]$seq 
	 for (k in kseq) { masteridnew[k]= maxid }
	 masteridnew[i]= maxid
	 masteridnew[j]= maxid
        description[i]="Master"
        description[j]="Name and Address same"
      }
    }
    else if(d>0){
      if(amatch(Name[i],Name[j], method="dl", maxDist=2, nomatch=0)[[1]] > 0){
         Mastid  <- NULL
	 Mastid  <- data.frame(masteridnew)
	 colnames(Mastid) <- c("Mid")
	 Mastid$seq <- 1:nrow(Mastid)
	 maxid = max(masteridnew[i],masteridnew[j])
	 kseq <- Mastid[(Mastid$Mid==masteridnew[i]),]$seq 
	 for (k in kseq) { masteridnew[k]= maxid }
	 kseq <- Mastid[(Mastid$Mid==masteridnew[j]),]$seq 
	 for (k in kseq) { masteridnew[k]= maxid }
	 masteridnew[i]= maxid
	 masteridnew[j]= maxid
        description[i]="Master"
        description[j]="Name and Address same"
      }
    }

  j <- j+1
  c <- match(Name[i], Name[j], nomatch=0)
  if (c == 0) { c <- amatch(Name[i], Name[j], method="dl", maxDist=2, nomatch=0) }
  }
}
dataexm$masteridnew <- masteridnew
dataexm$description <- description
return(dataexm[,5:6])
}


#This function takes in as input dataframe (sorted by Address)
#Returns updated Masterid, Description vectors (based on Match tagging logic)

ParFn_addr_match <- function(dataexm)
{
lendataexm <- nrow(dataexm)
Phone   <- dataexm$PHONE_NUMBER_new
Name    <- dataexm$CUSTOMER_NAME_new
Address <- dataexm$ADDRESS_new
masteridnew <- dataexm$masteridnew
description <- dataexm$description
for(i in (1:(lendataexm-1))) {
  print(i)
  j <- i+1
  d <- match(Address[i], Address[j], nomatch=0)
  if (d == 0) { d <- amatch(Address[i], Address[j], method="dl", maxDist=10, nomatch=0) }
## Perform inner loop (if there is either Exact or Approximate match on Address) 
  while ((d > 0) & (j <= lendataexm)) {

    b <- match(Phone[i],Phone[j],nomatch = 0)
    c <- match(Name[i],Name[j],nomatch = 0)
    d <- match(Address[i],Address[j],nomatch = 0)
    
    if(b>0){
      if(d>0){
        if(c>0){
          Mastid  <- NULL
	  Mastid  <- data.frame(masteridnew)
	  colnames(Mastid) <- c("Mid")
	  Mastid$seq <- 1:nrow(Mastid)
	  maxid = max(masteridnew[i],masteridnew[j])
	  kseq <- Mastid[(Mastid$Mid==masteridnew[i]),]$seq 
	  for (k in kseq) { masteridnew[k]= maxid }
	  kseq <- Mastid[(Mastid$Mid==masteridnew[j]),]$seq 
	  for (k in kseq) { masteridnew[k]= maxid }
	  masteridnew[i]= maxid
	  masteridnew[j]= maxid
         description[i]="Master"
         description[j]="Same customer"
        }
        else if(amatch(Name[i],Name[j], method="dl", maxDist=2, nomatch=0)[[1]] > 0){
          Mastid  <- NULL
 	  Mastid  <- data.frame(masteridnew)
	  colnames(Mastid) <- c("Mid")
	  Mastid$seq <- 1:nrow(Mastid)
	  maxid = max(masteridnew[i],masteridnew[j])
	  kseq <- Mastid[(Mastid$Mid==masteridnew[i]),]$seq 
	  for (k in kseq) { masteridnew[k]= maxid }
	  kseq <- Mastid[(Mastid$Mid==masteridnew[j]),]$seq 
	  for (k in kseq) { masteridnew[k]= maxid }
	  masteridnew[i]= maxid
	  masteridnew[j]= maxid
         description[i]="Master"
         description[j]="Same customer"
        }
        else{
           Mastid  <- NULL
 	   Mastid  <- data.frame(masteridnew)
	   colnames(Mastid) <- c("Mid")
	   Mastid$seq <- 1:nrow(Mastid)
	   maxid = max(masteridnew[i],masteridnew[j])
	   kseq <- Mastid[(Mastid$Mid==masteridnew[i]),]$seq 
	   for (k in kseq) { masteridnew[k]= maxid }
	   kseq <- Mastid[(Mastid$Mid==masteridnew[j]),]$seq 
	   for (k in kseq) { masteridnew[k]= maxid }
	   masteridnew[i]= maxid
	   masteridnew[j]= maxid
          description[i]="Master"
          description[j]="Relation"
        }
      }
      else if(c>0){
        if(amatch(Address[i],Address[j], method="dl", maxDist=10, nomatch=0)[[1]] > 0){
           Mastid  <- NULL
	   Mastid  <- data.frame(masteridnew)
	   colnames(Mastid) <- c("Mid")
	   Mastid$seq <- 1:nrow(Mastid)
	   maxid = max(masteridnew[i],masteridnew[j])
	   kseq <- Mastid[(Mastid$Mid==masteridnew[i]),]$seq 
	   for (k in kseq) { masteridnew[k]= maxid }
	   kseq <- Mastid[(Mastid$Mid==masteridnew[j]),]$seq 
	   for (k in kseq) { masteridnew[k]= maxid }
	   masteridnew[i]= maxid
	   masteridnew[j]= maxid
          description[i]="Master"
          description[j]="Same customer"
        }
        else{
           Mastid  <- NULL
 	   Mastid  <- data.frame(masteridnew)
	   colnames(Mastid) <- c("Mid")
	   Mastid$seq <- 1:nrow(Mastid)
	   maxid = max(masteridnew[i],masteridnew[j])
	   kseq <- Mastid[(Mastid$Mid==masteridnew[i]),]$seq 
	   for (k in kseq) { masteridnew[k]= maxid }
	   kseq <- Mastid[(Mastid$Mid==masteridnew[j]),]$seq 
	   for (k in kseq) { masteridnew[k]= maxid }
	   masteridnew[i]= maxid
	   masteridnew[j]= maxid
          description[i]="Master"
          description[j]="Name and Phone number same"
        }
      }
      else if(amatch(Address[i],Address[j], method="dl", maxDist=10, nomatch=0)[[1]] > 0){
         Mastid  <- NULL
 	 Mastid  <- data.frame(masteridnew)
	 colnames(Mastid) <- c("Mid")
	 Mastid$seq <- 1:nrow(Mastid)
	 maxid = max(masteridnew[i],masteridnew[j])
	 kseq <- Mastid[(Mastid$Mid==masteridnew[i]),]$seq 
	 for (k in kseq) { masteridnew[k]= maxid }
	 kseq <- Mastid[(Mastid$Mid==masteridnew[j]),]$seq 
	 for (k in kseq) { masteridnew[k]= maxid }
	 masteridnew[i]= maxid
	 masteridnew[j]= maxid
        description[i]="Master"
        description[j]="Relation"
      }
      else if(amatch(Name[i],Name[j], method="dl", maxDist=2, nomatch=0)[[1]] > 0){
        if(amatch(Address[i],Address[j], method="dl", maxDist=10, nomatch=0)[[1]] > 0){
           Mastid  <- NULL
	   Mastid  <- data.frame(masteridnew)
	   colnames(Mastid) <- c("Mid")
	   Mastid$seq <- 1:nrow(Mastid)
	   maxid = max(masteridnew[i],masteridnew[j])
	   kseq <- Mastid[(Mastid$Mid==masteridnew[i]),]$seq 
	   for (k in kseq) { masteridnew[k]= maxid }
	   kseq <- Mastid[(Mastid$Mid==masteridnew[j]),]$seq 
	   for (k in kseq) { masteridnew[k]= maxid }
	   masteridnew[i]= maxid
	   masteridnew[j]= maxid
          description[i]="Master"
          description[j]="Same customer"
        }
        else{  
           Mastid  <- NULL
	   Mastid  <- data.frame(masteridnew)
	   colnames(Mastid) <- c("Mid")
	   Mastid$seq <- 1:nrow(Mastid)
	   maxid = max(masteridnew[i],masteridnew[j])
	   kseq <- Mastid[(Mastid$Mid==masteridnew[i]),]$seq 
	   for (k in kseq) { masteridnew[k]= maxid }
	   kseq <- Mastid[(Mastid$Mid==masteridnew[j]),]$seq 
	   for (k in kseq) { masteridnew[k]= maxid }
	   masteridnew[i]= maxid
	   masteridnew[j]= maxid
          description[i]="Master"
          description[j]="Name and Phone number same"
        }
      }
    }
    else if(c>0){
      if(d>0){
         Mastid  <- NULL
	 Mastid  <- data.frame(masteridnew)
	 colnames(Mastid) <- c("Mid")
	 Mastid$seq <- 1:nrow(Mastid)
	 maxid = max(masteridnew[i],masteridnew[j])
	 kseq <- Mastid[(Mastid$Mid==masteridnew[i]),]$seq 
	 for (k in kseq) { masteridnew[k]= maxid }
	 kseq <- Mastid[(Mastid$Mid==masteridnew[j]),]$seq 
	 for (k in kseq) { masteridnew[k]= maxid }
	 masteridnew[i]= maxid
	 masteridnew[j]= maxid
        description[i]="Master"
        description[j]="Name and Address same"
      }
      else if(amatch(Address[i],Address[j], method="dl", maxDist=10, nomatch=0)[[1]] > 0){
         Mastid  <- NULL
	 Mastid  <- data.frame(masteridnew)
	 colnames(Mastid) <- c("Mid")
	 Mastid$seq <- 1:nrow(Mastid)
	 maxid = max(masteridnew[i],masteridnew[j])
	 kseq <- Mastid[(Mastid$Mid==masteridnew[i]),]$seq 
	 for (k in kseq) { masteridnew[k]= maxid }
	 kseq <- Mastid[(Mastid$Mid==masteridnew[j]),]$seq 
	 for (k in kseq) { masteridnew[k]= maxid }
	 masteridnew[i]= maxid
	 masteridnew[j]= maxid
        description[i]="Master"
        description[j]="Name and Address same"
      }
    }
    else if(d>0){
      if(amatch(Name[i],Name[j], method="dl", maxDist=2, nomatch=0)[[1]] > 0){
         Mastid  <- NULL
	 Mastid  <- data.frame(masteridnew)
	 colnames(Mastid) <- c("Mid")
	 Mastid$seq <- 1:nrow(Mastid)
	 maxid = max(masteridnew[i],masteridnew[j])
	 kseq <- Mastid[(Mastid$Mid==masteridnew[i]),]$seq 
	 for (k in kseq) { masteridnew[k]= maxid }
	 kseq <- Mastid[(Mastid$Mid==masteridnew[j]),]$seq 
	 for (k in kseq) { masteridnew[k]= maxid }
	 masteridnew[i]= maxid
	 masteridnew[j]= maxid
        description[i]="Master"
        description[j]="Name and Address same"
      }
    }

    j <- j+1
    d <- match(Address[i], Address[j], nomatch=0)
    if (d == 0) { d <- amatch(Address[i], Address[j], method="dl", maxDist=10, nomatch=0) }
  }
}
dataexm$masteridnew <- masteridnew
dataexm$description <- description
return(dataexm[,5:6])
}
