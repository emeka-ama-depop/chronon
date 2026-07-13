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


def test_flink_api_props_from_conf_handles_null_nested_config(tmp_path):
    for index, payload in enumerate(
        [
            {"metaData": None},
            {"metaData": {"executionInfo": None}},
            {"metaData": {"executionInfo": {"conf": None}}},
            {"metaData": {"executionInfo": {"conf": {"common": None}}}},
        ]
    ):
        conf_path = tmp_path / f"groupby_{index}.json"
        conf_path.write_text(json.dumps(payload))

        assert AwsRunner.flink_api_props_from_conf(str(conf_path)) == {}
