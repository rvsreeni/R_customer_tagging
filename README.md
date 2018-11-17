### R_Customer_Tagging 
#### Group related customers by tagging them with common id, based on attributes like Phone, Name, Address)

#### Reads in the input file
#### Eliminate Duplicates (on Customer Number) - write Duplicates to output file
#### Validate - customer Name, Phone - write Invalid entries to output files
#### Match Tagging logic
#####    Iteration #1    - Sort on Phone#, divide the input into 40 chunks, process them in parallel(4 chunks at a time)
#####                    - Calls CDBcleanup_fullpar_phone_match_upd 
#####                    - Calls ParFn_phone_bndchk  
#####                    - Calls function ParFn_phone_match 
#####                    - Update (MasterId, Description) 
#####    Iteration #2.1  - Sort on Name, divide the input into 40 chunks, process them in parallel(4 chunks at a time)
#####	                   - Calls CDBcleanup_fullpar_name_match_upd
#####	                   - Calls ParFn_name_bndchk
#####	                   - Calls function ParFn_name_match to do Match Tagging on Phone, Name, Address
#####	                   - Update (MasterId, Description) 
#####    Iteration #2.2  - Sort on Reversed Name, divide the input into 40 chunks, process them in parallel(4 chunks at a time)
#####	                   - Calls CDBcleanup_fullpar_name_match_upd
#####	                   - Calls ParFn_name_bndchk
#####	                   - Calls function ParFn_name_match to do Match Tagging on Phone, Reversed Name, Address
#####	                   - Update (MasterId, Description) 
#####    Itera####tion #3.1  - Sort on Address, divide the input into 40 chunks, process them in parallel(4 chunks at a time)
#####                    - Calls CDBcleanup_fullpar_addr_match_upd
#####                    - Calls ParFn_addr_bndchk
#####                    - Calls function ParFn_addr_match to do Match Tagging on Phone, Name, Address               
#####                    - Update (MasterId, Description) 
#####    Iteration #3.2  - Sort on Reversed Address, divide the input into 40 chunks, process them in parallel(4 chunks at a time)
#####                    - Calls CDBcleanup_fullpar_addr_match_upd
#####                    - Calls ParFn_addr_bndchk
#####                    - Calls function ParFn_addr_match to do Match Tagging on Phone, Name, Reversed Address    
#####                    - Update (MasterId, Description) 
#### Link Masterid of related customers
#####     - Split data into separate chunks, process them in parallel(4 chunks at a time)
#####     - Calls function ParFullFn_linkmid to link Masterid of related customers
#### Write to Output file (input file fields, plus masterid, descr
