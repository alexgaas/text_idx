## Prepare dataset for testing
[Testing been performed on Britannica data corpus](https://nlsfoundry.s3.amazonaws.com/text/nls-text-encyclopaediaBritannica.zip)
(file size is 336 MB compressed / 946.88 MB uncompressed).

- from the root of project download:
```text
wget https://nlsfoundry.s3.amazonaws.com/text/nls-text-encyclopaediaBritannica.zip
```
- then unzip
```text
unzip nls-text-encyclopaediaBritannica.zip
```
- remove contents csv file
```text
rm -rf nls-text-encyclopaediaBritannica/encyclopaediaBritannica-inventory.csv
rm -rf nls-text-encyclopaediaBritannica/readme.csv
```
- copy to `Resources` folder
```text
cp -a nls-text-encyclopaediaBritannica/. src/test/resources
```
- clean up after
```text
rm -rf nls-text-encyclopaediaBritannica.zip
rm -rfd nls-text-encyclopaediaBritannica
```
