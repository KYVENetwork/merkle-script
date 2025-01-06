import gzip
import io
import json
import os
import sys

import requests
import logging

from hashlib import sha256


## Pools
# #   Pool       Runtime     To BundleId (inclusive)
# 0: Cosmos   (bsync)
# 1: Osmosis  (tendermint)
# 2: Archway  (tendermint)  206232
# 3: Axelar   (tendermint)
# 5: Cronos   (tendermint)
# 7: Noble    (tendermint)
# 9: Celestia (tendermint)  68399


def merkle_root(hashes):
    if len(hashes) == 0:
        raise ValueError('hashes cannot be empty')

    if len(hashes) % 2 != 0:
        hashes.append(hashes[-1])

    next_hashes = []
    for i in range(0, len(hashes), 2):
        next_hashes.append(sha256(bytes.fromhex(hashes[i] + hashes[i+1])).hexdigest())

    if len(next_hashes) == 1:
        return next_hashes[0]

    return merkle_root(next_hashes)


def tendermint_merkle_root(bundle_content):
    return merkle_root([
        merkle_root([
            sha256(item["key"].encode('utf-8')).hexdigest(),
            merkle_root(
                [
                    sha256(json.dumps(item["value"]["block"], separators=(',', ':')).encode('utf-8')).hexdigest(),
                    sha256(
                        json.dumps(item["value"]["block_results"], separators=(',', ':')).encode('utf-8')).hexdigest()
                ]
            )
        ])
        for item in bundle_content
    ])


def iterate_pool(pool_id):
    outfile = "merkle_roots_pool_" + str(pool_id)
    offset = os.path.getsize(outfile) // 32 if os.path.exists(outfile) else 0
    logging.info("Starting. pool_id=%s offset=%s", pool_id, offset)
    bundles_finished = False
    while not bundles_finished:
        r = requests.get("https://api.kyve.network/kyve/v1/bundles/{}?pagination.limit=10&pagination.offset={}".format(pool_id, offset))
        bundles = r.json()["finalized_bundles"]
        merkle_hashes = []
        for bundle in bundles:
            if "merkle_root" in bundle["bundle_summary"]:
                bundles_finished = True
                break

            raw_bundle = requests.get("https://arweave.net/" + bundle["storage_id"]).content
            if sha256(raw_bundle).hexdigest() != bundle["data_hash"]:
                raise ValueError("Invalid Bundle hash: " + str(bundle))

            merkle_hashes.append(tendermint_merkle_root(json.load(gzip.GzipFile(fileobj=io.BytesIO(raw_bundle)))))

        with open(outfile, "ba") as f:
            f.write(bytes.fromhex("".join(merkle_hashes)))

        offset += len(merkle_hashes)
        logging.info("Written.  pool_id=%s to_bundle_id(incl.)=%s", pool_id, offset - 1)

    logging.info("Finished. pool_id=%s", pool_id)


logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
if len(sys.argv) != 2:
    print("Usage: merkle_generator.py <pool_id>")
    exit(1)

iterate_pool(int(sys.argv[1]))
