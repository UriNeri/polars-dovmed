# Patterns
NOTE I AM CHANGING STUFF ON THE GO, PARTS OF THIS FILE ARE PROBABLY OUT OF DATE  


## `identifiers.json`

### `refseq`
[two-letter alphabetical prefix][ _ ][series of digits or alphanumeric characters][.][version number]

|Accession prefix|Molecule type|Comment|
|---|---|---|
|AC_|Genomic|Complete genomic molecule, usually alternate assembly|
|NC_|Genomic|Complete genomic molecule, usually reference assembly|
|NG_|Genomic|Incomplete genomic region|
|NT_|Genomic|Contig or scaffold, clone-based or WGSa|
|NW_|Genomic|Contig or scaffold, primarily WGSa|
|NZ_<sub>b</sub>|Genomic|Complete genomes and unfinished WGS data|
|NM_|mRNA|Protein-coding transcripts (usually curated)|
|NR_|RNA|Non-protein-coding transcripts|
|XM_<sub>c</sub>|mRNA|Predicted model protein-coding transcript|
|XR_<sub>c</sub>|RNA|Predicted model non-protein-coding transcript|
|AP_|Protein|Annotated on AC_ alternate assembly|
|NP_|Protein|Associated with an NM_ or NC_ accession|
|YP_<sub>c</sub>|Protein|Annotated on genomic molecules without an instantiated transcript record|
|XP_<sub>c</sub>|Protein|Predicted model, associated with an XM_ accession|
|WP_|Protein|Non-redundant across multiple strains and species|

<sub>a</sub> Whole Genome Shotgun sequence data.
<sub>b</sub> An ordered collection of WGS sequence for a genome.
<sub>c</sub> Computed.

Information:
- https://www.ncbi.nlm.nih.gov/books/NBK21091/table/ch18.T.refseq_accession_numbers_and_mole/?report=objectonly
- https://support.nlm.nih.gov/kbArticle/?pn=KA-03437

### `genbank`
[alphabetical prefix] [series of digits] [.] [version number]

Information:
- https://support.nlm.nih.gov/kbArticle/?pn=KA-03436
- https://www.ncbi.nlm.nih.gov/genbank/acc_prefix/

### General INSDC Accession Prefix Format

!INSDC SPECIFIC! format for accession numbers:

#### Nucleotide:
Nucleotide:
       1 letter + 5 digits
       2 letters + 6 digits
       2 letters + 8 digits

Protein:     
       3 letters + 5 digits
       3 letters + 7 digits

WGS/TSA/TLS:   
       4 letters + 2 digits for assembly version + 6 or more digits
       6 letters + 2 digits for assembly version + 7 or more digits

SRA (Sequence Read Archive):
       3 letters (see prefix below) + 6 or more digits

BioSample:
       4 letters (see prefix below) + 8 or more digits

BioProject:
       5 letters (see prefix below) + 1 or more digits

MGA:           
       5 letters + 7 digits

### Specific accession patterns for different databases

Table below is from:
- https://www.ncbi.nlm.nih.gov/genbank/acc_prefix/

| Accession Prefix | INSDC Partner | Sequence Type              | Accession Format |
| ---------------- | ------------- | -------------------------- | ---------------- |
| A                | EBI           | Patent                     | 1+5              |  |
| AA               | NCBI          | EST                        | 2+6              |  |
| AAA-AZZ          | NCBI          | protein                    | 3+5 and 3+7      |  |
| AAAA-AZZZ        | NCBI          | WGS                        | 4+8 or more      |  |
| AAAAA-AZZZZ      | DDBJ          | MGA                        | 5+7              |  |
| AAAAAA-AZZZZZ    | NCBI          | WGS                        | 6+9 or more      |  |
| AB               | DDBJ          | Direct submissions         | 2+6              |  |
| AC               | NCBI          | HTG                        | 2+6              |  |
| AD               | NCBI          | Seqs received at GSDB      | 2+6              |  |
| AE               | NCBI          | Genome projects            | 2+6              |  |
| AF               | NCBI          | Direct submissions         | 2+6              |  |
| AG               | DDBJ          | GSS                        | 2+6              |  |
| AH               | NCBI          | Direct submissions segsets | 2+6              |  |
| AI               | NCBI          | Other projects             | 2+6              |  |
| AJ               | EBI           | Direct submissions         | 2+6              |  |
| AK               | DDBJ          | HTC                        | 2+6              |  |
| AL               | EBI           | Genome projects            | 2+6              |  |
| AM               | EBI           | Direct submissions         | 2+6              |  |
| AN               | EBI           | scaffold/CON               | 2+6              |  |
| AP               | DDBJ          | Genome projects            | 2+6              |  |
| AQ               | NCBI          | GSS                        | 2+6              |  |
| AR               | NCBI          | Patent                     | 2+6              |  |
| AS               | NCBI          | Other projects             | 2+6              |  |
| AT               | DDBJ          | EST                        | 2+6              |  |
| AU               | DDBJ          | EST                        | 2+6              |  |
| AV               | DDBJ          | EST                        | 2+6              |  |
| AW               | NCBI          | EST                        | 2+6              |  |
| AX               | EBI           | Patent                     | 2+6              |  |
| AY               | NCBI          | Direct submissions         | 2+6              |  |
| AZ               | NCBI          | GSS                        | 2+6              |  |
| B                | NCBI          | GSS (previously DDBJ)      | 1+5              |  |
| BA               | DDBJ          | scaffold/CON               | 2+6              |  |
| BAA-BZZ          | DDBJ          | protein                    | 3+5 and 3+7      |  |
| BAAA-BZZZ        | DDBJ          | WGS                        | 4+8 or more      |  |
| BAAAAA-BZZZZZ    | DDBJ          | WGS                        | 6+9 or more      |  |
| BB               | DDBJ          | EST                        | 2+6              |  |
| BC               | NCBI          | cDNA project               | 2+6              |  |
| BD               | DDBJ          | Patent                     | 2+6              |  |
| BE               | NCBI          | EST                        | 2+6              |  |
| BF               | NCBI          | EST                        | 2+6              |  |
| BG               | NCBI          | EST                        | 2+6              |  |
| BH               | NCBI          | GSS                        | 2+6              |  |
| BI               | NCBI          | EST                        | 2+6              |  |
| BJ               | DDBJ          | EST                        | 2+6              |  |
| BK               | NCBI          | TPA                        | 2+6              |  |
| BL               | NCBI          | TPA CON                    | 2+6              |  |
| BM               | NCBI          | EST                        | 2+6              |  |
| BN               | EBI           | TPA                        | 2+6              |  |
| BP               | DDBJ          | EST                        | 2+6              |  |
| BQ               | NCBI          | EST                        | 2+6              |  |
| BR               | DDBJ          | TPA                        | 2+6              |  |
| BS               | DDBJ          | Genome projects            | 2+6              |  |
| BT               | NCBI          | FLI_cDNA                   | 2+6              |  |
| BU               | NCBI          | EST                        | 2+6              |  |
| BV               | NCBI          | STS                        | 2+6              |  |
| BW               | DDBJ          | EST                        | 2+6              |  |
| BX               | EBI           | Genome projects            | 2+6              |  |
| BY               | DDBJ          | EST                        | 2+6              |  |
| BZ               | NCBI          | GSS                        | 2+6              |  |
| C                | DDBJ          | EST                        | 1+5              |  |
| CA               | NCBI          | EST                        | 2+6              |  |
| CAA-CZZ          | EBI           | protein                    | 3+5 and 3+7      |  |
| CAAA-CZZZ        | EBI           | WGS                        | 4+8 or more      |  |
| CAAAAA-CZZZZZ    | EBI           | WGS                        | 6+9 or more      |  |
| CB               | NCBI          | EST                        | 2+6              |  |
| CC               | NCBI          | GSS                        | 2+6              |  |
| CD               | NCBI          | EST                        | 2+6              |  |
| CE               | NCBI          | GSS                        | 2+6              |  |
| CF               | NCBI          | EST                        | 2+6              |  |
| CG               | NCBI          | GSS                        | 2+6              |  |
| CH               | NCBI          | scaffold/CON               | 2+6              |  |
| CI               | DDBJ          | EST                        | 2+6              |  |
| CJ               | DDBJ          | EST                        | 2+6              |  |
| CK               | NCBI          | EST                        | 2+6              |  |
| CL               | NCBI          | GSS                        | 2+6              |  |
| CM               | NCBI          | scaffold/CON               | 2+6              |  |
| CN               | NCBI          | EST                        | 2+6              |  |
| CO               | NCBI          | EST                        | 2+6              |  |
| CP               | NCBI          | Genome projects            | 2+6              |  |
| CQ               | EBI           | Patent                     | 2+6              |  |
| CR               | EBI           | Genome projects            | 2+6              |  |
| CS               | EBI           | Patent                     | 2+6              |  |
| CT               | EBI           | Genome projects            | 2+6              |  |
| CU               | EBI           | Genome projects            | 2+6              |  |
| CV               | NCBI          | EST                        | 2+6              |  |
| CW               | NCBI          | GSS                        | 2+6              |  |
| CX               | NCBI          | EST                        | 2+6              |  |
| CY               | NCBI          | Influenza Virus Genome     | 2+6              |  |
| CZ               | NCBI          | GSS                        | 2+6              |  |
| D                | DDBJ          | Direct submissions         | 1+5              |  |
| DA               | DDBJ          | EST                        | 2+6              |  |
| DAA-DZZ          | NCBI          | TPA or TPA WGS protein     | 3+5 and 3+7      |  |
| DAAA-DZZZ        | NCBI          | WGS/TSA/TLS TPA            | 4+8 or more      |  |
| DAAAAA-DZZZZZ    | NCBI          | WGS/TSA/TLS TPA            | 6+9 or more      |  |
| DB               | DDBJ          | EST                        | 2+6              |  |
| DC               | DDBJ          | EST                        | 2+6              |  |
| DD               | DDBJ          | Patent                     | 2+6              |  |
| DE               | DDBJ          | GSS                        | 2+6              |  |
| DF               | DDBJ          | scaffold/CON               | 2+6              |  |
| DG               | DDBJ          | scaffold/CON               | 2+6              |  |
| DH               | DDBJ          | GSS                        | 2+6              |  |
| DI               | DDBJ          | Patent KIPO                | 2+6              |  |
| DJ               | DDBJ          | Patent JPO                 | 2+6              |  |
| DK               | DDBJ          | EST                        | 2+6              |  |
| DL               | DDBJ          | Patent JPO                 | 2+6              |  |
| DM               | DDBJ          | Patent JPO                 | 2+6              |  |
| DN               | NCBI          | EST                        | 2+6              |  |
| DO               | NCBI          | not used                   | 2+6              |  |
| DP               | NCBI          | HTG scaffolds (CONs)       | 2+6              |  |
| DQ               | NCBI          | Direct submissions         | 2+6              |  |
| DR               | NCBI          | EST                        | 2+6              |  |
| DRA              | DDBJ          | SRA submissions            | 3+6 or more      |  |
| DRP              | DDBJ          | SRA sample                 | 3+6 or more      |  |
| DRR              | DDBJ          | SRA runs                   | 3+6 or more      |  |
| DRX              | DDBJ          | SRA experiment             | 3+6 or more      |  |
| DRZ              | DDBJ          | SRA analysis object        | 3+6 or more      |  |
| DS               | NCBI          | scaffold/CON               | 2+6              |  |
| DT               | NCBI          | EST                        | 2+6              |  |
| DU               | NCBI          | GSS                        | 2+6              |  |
| DV               | NCBI          | EST                        | 2+6              |  |
| DW               | NCBI          | EST                        | 2+6              |  |
| DX               | NCBI          | GSS                        | 2+6              |  |
| DY               | NCBI          | EST                        | 2+6              |  |
| DZ               | NCBI          | Patent                     | 2+6              |  |
| E                | DDBJ          | Patent                     | 1+5              |  |
| EA               | NCBI          | Patent                     | 2+6              |  |
| EAA-EZZ          | NCBI          | WGS protein                | 3+5 and 3+7      |  |
| EAAA-EZZZ        | DDBJ          | WGS TPA                    | 4+8 or more      |  |
| EB               | NCBI          | EST                        | 2+6              |  |
| EC               | NCBI          | EST                        | 2+6              |  |
| ED               | NCBI          | GSS                        | 2+6              |  |
| EE               | NCBI          | EST                        | 2+6              |  |
| EF               | NCBI          | Direct submissions         | 2+6              |  |
| EG               | NCBI          | EST                        | 2+6              |  |
| EH               | NCBI          | EST                        | 2+6              |  |
| EI               | NCBI          | GSS                        | 2+6              |  |
| EJ               | NCBI          | GSS                        | 2+6              |  |
| EK               | NCBI          | GSS                        | 2+6              |  |
| EL               | NCBI          | EST                        | 2+6              |  |
| EM               | NCBI          | scaffold/CON               | 2+6              |  |
| EN               | NCBI          | scaffold/CON               | 2+6              |  |

### `general_accessions`
Names of databases that often precede database accessions, for example:
- `accession number`
- `accession ID`
- `accession code`
- `assembly accession number`
- `assembly accession ID`
- `assembly accession code`
- `genome accession number`
- `genome accession ID`
- `genome accession code`
- `protein accession number`
- `protein accession ID`
- `protein accession code`


## `coordinates.json`
Genomic and protein coordinate patterns, for example:
- `nucleotides 123456-123456`
- `positions 123456-123456`
- `bp 123456-123456`
- `nt 123456-123456`
- `amino acids 123456-123456`
- `region: 123456-123456`
- `span 123456-123456`
- `range 123456-123456`

## `taxonomy.json`
NCBI Taxonomy ID patterns, for example:  
- `taxid: 123456`
- `taxonomy ID: 123456`
- `taxonomy identifier: 123456`

## `databases.json`
Names of common databases and repositories, for example:
- `GenBank`
- `NCBI`
- `RefSeq`
- `ENA`
- `BioProject`
- `BioSample`
- `UniProt`
- `SwissProt`
- `TrEMBL`
- `PDB`
- `...`

## `regex_queries.json`
Currently there are ~9 patterns for viral RNA structural elements.

## `manuscripts.json`
Patterns for DOIs, PubMed IDs, PMC IDs, and URLs (http, ftp, www), for example:
- `DOI: 10.1016/j.cell.2021.05.010`
- `PMID: 34060000`
- `PMCID: 123456`
- `https://www.ncbi.nlm.nih.gov/pubmed/34060000`

## `obsolete.ndjson`
- sra patterns (I don't think these are really useful, as implies needing to assemble the data)
- data_availability_keywords (too generic and can relate to data not generated in the paper)