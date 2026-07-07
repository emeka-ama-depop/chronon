import json

from ai.chronon.repo.aws import AwsRunner


def test_flink_api_props_from_conf_extracts_kv_common_conf(tmp_path):
    conf_path = tmp_path / "groupby.json"
    conf_path.write_text(
        json.dumps(
            {
                "metaData": {
                    "executionInfo": {
                        "conf": {
                            "common": {
                                "kv.enableDax": True,
                                "kv.daxEndpoint": "dax://example",
                                "spark.sql.shuffle.partitions": "200",
                            }
                        }
                    }
                }
            }
        )
    )

    assert AwsRunner.flink_api_props_from_conf(str(conf_path)) == {
        "-Zkv.enableDax": "true",
        "-Zkv.daxEndpoint": "dax://example",
    }
