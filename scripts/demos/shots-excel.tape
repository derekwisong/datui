# An Excel workbook by URL, Apache POI's SampleSS.xlsx: the download question,
# then T, the worksheet picker with each sheet's range and size.
# Page: formats/columnar-and-json.md (Excel).

Hide
Type "clear; datui https://raw.githubusercontent.com/apache/poi/trunk/test-data/spreadsheet/SampleSS.xlsx"
Enter
Wait+Screen /Continue with download/
Show

Sleep 1s
Enter
Wait+Screen /1 \/ 3/
Sleep 500ms
Type "T"
Wait+Screen /Sheet Number 2/
Sleep 1s
Screenshot excel-tables.png
