# 
#function to get the size of an ftp directory

output_path=$1
echo fetching to: $output_path
cd $output_path
mkdir oa_noncomm oa_comm oa_other
function get_ftp_size() {
    local ftp_url=$1
    curl -s $ftp_url | awk '{print $5}' | awk '{sum += $0} END {print sum}' |  numfmt --to=iec --format="%.2f"
}

oa_ftps=(
    "ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_bulk/oa_noncomm/xml/"
    "ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_bulk/oa_comm/xml/"
    "ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_bulk/oa_other/xml/"
)

for ftp in "${oa_ftps[@]}"; do
    echo "$ftp"
    get_ftp_size "$ftp"
done

# ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_bulk/oa_noncomm/xml/
# 30.07G
# ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_bulk/oa_comm/xml/
# 94.61G
# ftp:///ftp.ncbi.nlm.nih.gov/pub/pmc/oa_bulk/oa_other/xml/
# 5.53G

# function to get a list of all the files in the ftp directory to prepare for aria2 download
function get_ftp_files() {
    local ftp_url=$1
    local output_file=$2
    # fetch the file list, filter out lines that don't look like files, and prepend the ftp url
    curl -s "$ftp_url" | awk '{if ($9 ~ /\.tar\.gz$|\.xml\.gz$|\.csv$|\.txt$/) print $9}' | sed "s|^|$ftp_url|" > "$output_file"
}

cd data/

for ftp in "${oa_ftps[@]}"; do
    subset_name=$(echo "$ftp" | rev | cut -d'/' -f3 | rev)
    file_list="file_list_${subset_name}.txt"
    echo "$ftp"
    echo "$file_list"
    get_ftp_files "$ftp" "$file_list"
done

# function to only download all the stuff with aria2 and move the download to the background so I can exit the terminal
# and then I can check the download progress with by lookin at the log files
function aria2_download() {
        local ftp_url=$1
        local log_file=$2
    aria2c -c -i $ftp_url > $log_file 2>&1 & # nohup
}

for ftp in "${oa_ftps[@]}"; do
    subset_name=$(echo "$ftp" | rev | cut -d'/' -f3 | rev)
    file_list="file_list_"$subset_name".txt"
    log_file="get_oa_ftp_$subset_name.log"
    aria2_download "$file_list" "$log_file"
done