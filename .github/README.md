# merkle-script
This script can be used to reconstruct the Merkle root for already archived bundles.

## Computed Merkle roots
The following Merkle roots have been calculated for the KYVE chain v6 upgrade. 
Both Python and Go implementations were used to compute the hashes,  which provided identical results:

```
merkle_roots_pool_0
da4bb9bf0a60c5c79e399d8bb54ae4cf916f6c1dbdd5cdae45cb991f4e56158f

merkle_roots_pool_1
3c4eeb915cd01c6adea3241ea3536dfce5cec87017557b7e43d92c6ceec3096e  

merkle_roots_pool_2
754eb4680fe550cd3a7277ab0fc12c8f7ce794d18ca71d247561e40b05629c39  

merkle_roots_pool_3
df26b886928dbec03e84eca9b41c02b15ae7c5e7cf39ab540fcf381d3e1d27cc  

merkle_roots_pool_5
051efd6e44d7ac5bca41abb20aaf79d34dd095b5d6797d536bf13face7e397f9  

merkle_roots_pool_7
303d5ccaa18cc9e23298d599e3ba4c5bcf46f44d0fb5dd2cfdebcd02dcd8dc95 

merkle_roots_pool_9
e2f1c174350e5925d3f61b7adfb077f38507aec1562900b79c645099809ae617 
```

## Build from Source

```bash
git clone https://github.com/KYVENetwork/merkle-script.git

cd merkle-script

make

cp build/merkle-script ~/go/bin/merkle-script
```

## Usage

```bash
merkle-script start
```