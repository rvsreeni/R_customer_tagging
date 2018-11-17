#This script has boundary check functions (Phone, Name, Address)

#This function does not take any input, nor does it return anything
#It adjusts the boundaries of the chunks, based on the address

ParFn_addr_bndchk <- function() {
# Check boundaries of chunks 1,2 to see if the same Address is spread across both chunks, 
# If same Address is spread, then modify chunks 1,2 (so that Address is present only in one of the chunks)
n14_len = nrow(basedatawrt14)
n24_len = nrow(basedatawrt24)
n14_laddr = basedatawrt14$ADDRESS_new[n14_len]
n24_faddr = basedatawrt24$ADDRESS_new[1]
if (n14_laddr == n24_faddr) {
   n14_dat = subset(basedatawrt14, basedatawrt14$ADDRESS_new == n14_laddr)
   n24_dat = subset(basedatawrt24, basedatawrt24$ADDRESS_new == n24_faddr)
   n14_cnt = nrow(n14_dat)
   n24_cnt = nrow(n24_dat)
   if (n14_cnt > n24_cnt) {
      basedatawrt14 = rbind(basedatawrt14, n24_dat)
      basedatawrt24 = subset(basedatawrt24, !(basedatawrt24$ADDRESS_new == n24_faddr))
   }
   else { 
      basedatawrt24 = rbind(n14_dat, basedatawrt24)
      basedatawrt14 = subset(basedatawrt14, !(basedatawrt14$ADDRESS_new == n14_laddr))
   }
}
# Check boundaries of chunks 2,3 to see if the same Address is spread across both chunks, 
# If same Address is spread, then modify chunks 2,3 (so that Address is present only in one of the chunks)
n24_len = nrow(basedatawrt24)
n34_len = nrow(basedatawrt34)
n24_laddr = basedatawrt24$ADDRESS_new[n24_len]
n34_faddr = basedatawrt34$ADDRESS_new[1]
if (n24_laddr == n34_faddr) {
   n24_dat = subset(basedatawrt24, basedatawrt24$ADDRESS_new == n24_laddr)
   n34_dat = subset(basedatawrt34, basedatawrt34$ADDRESS_new == n34_faddr)
   n24_cnt = nrow(n24_dat)
   n34_cnt = nrow(n34_dat)
   if (n24_cnt > n34_cnt) {
      basedatawrt24 = rbind(basedatawrt24, n34_dat)
      basedatawrt34 = subset(basedatawrt34, !(basedatawrt34$ADDRESS_new == n34_faddr))
   }
   else { 
      basedatawrt34 = rbind(n24_dat, basedatawrt34)
      basedatawrt24 = subset(basedatawrt24, !(basedatawrt24$ADDRESS_new == n24_laddr))
   }
}
# Check boundaries of chunks 3,4 to see if the same Address is spread across both chunks, 
# If same Address is spread, then modify chunks 3,4 (so that Address is present only in one of the chunks)
n34_len = nrow(basedatawrt34)
n44_len = nrow(basedatawrt44)
n34_laddr = basedatawrt34$ADDRESS_new[n34_len]
n44_faddr = basedatawrt44$ADDRESS_new[1]
if (n34_laddr == n44_faddr) {
   n34_dat = subset(basedatawrt34, basedatawrt34$ADDRESS_new == n34_laddr)
   n44_dat = subset(basedatawrt44, basedatawrt44$ADDRESS_new == n44_faddr)
   n34_cnt = nrow(n34_dat)
   n44_cnt = nrow(n44_dat)
   if (n34_cnt > n44_cnt) {
      basedatawrt34 = rbind(basedatawrt34, n44_dat)
      basedatawrt44 = subset(basedatawrt44, !(basedatawrt44$ADDRESS_new == n44_faddr))
   }
   else { 
      basedatawrt44 = rbind(n34_dat, basedatawrt44)
      basedatawrt34 = subset(basedatawrt34, !(basedatawrt34$ADDRESS_new == n34_laddr))
   }
}
}


#This function does not take any input, nor does it return anything
#It adjusts the boundaries of the chunks, based on the name

ParFn_name_bndchk <- function() {
# Check boundaries of chunks 1,2 to see if the same Name is spread across both chunks, 
# If same Name is spread, then modify chunks 1,2 (so that Name is present only in one of the chunks)
n14_len = nrow(basedatawrt14)
n24_len = nrow(basedatawrt24)
n14_lname = basedatawrt14$CUSTOMER_NAME_new[n14_len]
n24_fname = basedatawrt24$CUSTOMER_NAME_new[1]
if (n14_lname == n24_fname) {
   n14_dat = subset(basedatawrt14, basedatawrt14$CUSTOMER_NAME_new == n14_lname)
   n24_dat = subset(basedatawrt24, basedatawrt24$CUSTOMER_NAME_new == n24_fname)
   n14_cnt = nrow(n14_dat)
   n24_cnt = nrow(n24_dat)
   if (n14_cnt > n24_cnt) {
      basedatawrt14 = rbind(basedatawrt14, n24_dat)
      basedatawrt24 = subset(basedatawrt24, !(basedatawrt24$CUSTOMER_NAME_new == n24_fname))
   }
   else { 
      basedatawrt24 = rbind(n14_dat, basedatawrt24)
      basedatawrt14 = subset(basedatawrt14, !(basedatawrt14$CUSTOMER_NAME_new == n14_lname))
   }
}
# Check boundaries of chunks 2,3 to see if the same Name is spread across both chunks, 
# If same Name is spread, then modify chunks 2,3 (so that Name is present only in one of the chunks)
n24_len = nrow(basedatawrt24)
n34_len = nrow(basedatawrt34)
n24_lname = basedatawrt24$CUSTOMER_NAME_new[n24_len]
n34_fname = basedatawrt34$CUSTOMER_NAME_new[1]
if (n24_lname == n34_fname) {
   n24_dat = subset(basedatawrt24, basedatawrt24$CUSTOMER_NAME_new == n24_lname)
   n34_dat = subset(basedatawrt34, basedatawrt34$CUSTOMER_NAME_new == n34_fname)
   n24_cnt = nrow(n24_dat)
   n34_cnt = nrow(n34_dat)
   if (n24_cnt > n34_cnt) {
      basedatawrt24 = rbind(basedatawrt24, n34_dat)
      basedatawrt34 = subset(basedatawrt34, !(basedatawrt34$CUSTOMER_NAME_new == n34_fname))
   }
   else { 
      basedatawrt34 = rbind(n24_dat, basedatawrt34)
      basedatawrt24 = subset(basedatawrt24, !(basedatawrt24$CUSTOMER_NAME_new == n24_lname))
   }
}
# Check boundaries of chunks 3,4 to see if the same Name is spread across both chunks, 
# If same Name is spread, then modify chunks 3,4 (so that Name is present only in one of the chunks)
n34_len = nrow(basedatawrt34)
n44_len = nrow(basedatawrt44)
n34_lname = basedatawrt34$CUSTOMER_NAME_new[n34_len]
n44_fname = basedatawrt44$CUSTOMER_NAME_new[1]
if (n34_lname == n44_fname) {
   n34_dat = subset(basedatawrt34, basedatawrt34$CUSTOMER_NAME_new == n34_lname)
   n44_dat = subset(basedatawrt44, basedatawrt44$CUSTOMER_NAME_new == n44_fname)
   n34_cnt = nrow(n34_dat)
   n44_cnt = nrow(n44_dat)
   if (n34_cnt > n44_cnt) {
      basedatawrt34 = rbind(basedatawrt34, n44_dat)
      basedatawrt44 = subset(basedatawrt44, !(basedatawrt44$CUSTOMER_NAME_new == n44_fname))
   }
   else { 
      basedatawrt44 = rbind(n34_dat, basedatawrt44)
      basedatawrt34 = subset(basedatawrt34, !(basedatawrt34$CUSTOMER_NAME_new == n34_lname))
   }
}
}


#This function does not take any input, nor does it return anything
#It adjusts the boundaries of the chunks, based on the phone

ParFn_phone_bndchk <- function() {
# Check boundaries of chunks 1,2 to see if the same Phone# is spread across both chunks, 
# If same Phone# is spread, then modify chunks 1,2 (so that Phone# is present only in one of the chunks)
n14_len = nrow(basedatawrt14)
n24_len = nrow(basedatawrt24)
n14_lphone = basedatawrt14$PHONE_NUMBER_new[n14_len]
n24_fphone = basedatawrt24$PHONE_NUMBER_new[1]
if (n14_lphone == n24_fphone) {
   n14_dat = subset(basedatawrt14, basedatawrt14$PHONE_NUMBER_new == n14_lphone)
   n24_dat = subset(basedatawrt24, basedatawrt24$PHONE_NUMBER_new == n24_fphone)
   n14_cnt = nrow(n14_dat)
   n24_cnt = nrow(n24_dat)
   if (n14_cnt > n24_cnt) {
      basedatawrt14 = rbind(basedatawrt14, n24_dat)
      basedatawrt24 = subset(basedatawrt24, !(basedatawrt24$PHONE_NUMBER_new == n24_fphone))
   }
   else { 
      basedatawrt24 = rbind(n14_dat, basedatawrt24)
      basedatawrt14 = subset(basedatawrt14, !(basedatawrt14$PHONE_NUMBER_new == n14_lphone))
   }
}
# Check boundaries of chunks 2,3 to see if the same Phone# is spread across both chunks, 
# If same Phone# is spread, then modify chunks 2,3 (so that Phone# is present only in one of the chunks)
n24_len = nrow(basedatawrt24)
n34_len = nrow(basedatawrt34)
n24_lphone = basedatawrt24$PHONE_NUMBER_new[n24_len]
n34_fphone = basedatawrt34$PHONE_NUMBER_new[1]
if (n24_lphone == n34_fphone) {
   n24_dat = subset(basedatawrt24, basedatawrt24$PHONE_NUMBER_new == n24_lphone)
   n34_dat = subset(basedatawrt34, basedatawrt34$PHONE_NUMBER_new == n34_fphone)
   n24_cnt = nrow(n24_dat)
   n34_cnt = nrow(n34_dat)
   if (n24_cnt > n34_cnt) {
      basedatawrt24 = rbind(basedatawrt24, n34_dat)
      basedatawrt34 = subset(basedatawrt34, !(basedatawrt34$PHONE_NUMBER_new == n34_fphone))
   }
   else { 
      basedatawrt34 = rbind(n24_dat, basedatawrt34)
      basedatawrt24 = subset(basedatawrt24, !(basedatawrt24$PHONE_NUMBER_new == n24_lphone))
   }
}
# Check boundaries of chunks 3,4 to see if the same Phone# is spread across both chunks, 
# If same Phone# is spread, then modify chunks 3,4 (so that Phone# is present only in one of the chunks)
n34_len = nrow(basedatawrt34)
n44_len = nrow(basedatawrt44)
n34_lphone = basedatawrt34$PHONE_NUMBER_new[n34_len]
n44_fphone = basedatawrt44$PHONE_NUMBER_new[1]
if (n34_lphone == n44_fphone) {
   n34_dat = subset(basedatawrt34, basedatawrt34$PHONE_NUMBER_new == n34_lphone)
   n44_dat = subset(basedatawrt44, basedatawrt44$PHONE_NUMBER_new == n44_fphone)
   n34_cnt = nrow(n34_dat)
   n44_cnt = nrow(n44_dat)
   if (n34_cnt > n44_cnt) {
      basedatawrt34 = rbind(basedatawrt34, n44_dat)
      basedatawrt44 = subset(basedatawrt44, !(basedatawrt44$PHONE_NUMBER_new == n44_fphone))
   }
   else { 
      basedatawrt44 = rbind(n34_dat, basedatawrt44)
      basedatawrt34 = subset(basedatawrt34, !(basedatawrt34$PHONE_NUMBER_new == n34_lphone))
   }
}
}
