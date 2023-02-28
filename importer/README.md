# Import and process the 3D City data
As the index 3D City database makes it long time to import stuff, this importer devides xml data in batches, processes the data and recreates the database. So each bunch gets imported in an empty database which makes it faster.

## Usage
`./importer.sh <city name>`