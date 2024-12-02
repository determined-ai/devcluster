import devcluster as dc

# test deep_merge_configs
def test_deep_merge_configs():
    # Test merge dicts
    configs = [
        {
            "stages": [
                {"stage1": {"param1": 1, "param2": 2}},
                {"stage5": {"param2": 3, "param3": 4}},
                {"stage4": {"param2": 3, "param3": 4}}
            ]
        },
        {
            "stages": [
                {"stage1": {"param2": 3, "param3": 4}},
                {"stage3": {"param2": 3, "param3": 4}}
            ]
        },
    ]
    merged = dc.deep_merge_configs(configs)
    assert merged.get("stages")[0] == {"stage1": {"param1": 1, "param2": 3, "param3": 4}}

    # Test merge lists of dicts with the same pool_name
    configs = [
        {"pools": [{"pool_name": "pool1", "param1": 1}, {"pool_name": "pool2", "param1": 1}]},
        {"pools": [{"pool_name": "pool1", "param2": 2}, {"pool_name": "pool2", "param2": 2}]}
    ]
    merged = dc.deep_merge_configs(configs)
    assert merged == {"pools": [
        {"pool_name": "pool1", "param1": 1, "param2": 2},
        {"pool_name": "pool2", "param1": 1, "param2": 2}
    ]}

    # Test merge lists of dicts with different pool_names
    configs = [
        {"pools": [{"pool_name": "pool1", "param1": 1}, {"pool_name": "pool2", "param1": 1}]},
        {"pools": [{"pool_name": "pool3", "param1": 1}, {"pool_name": "pool4", "param1": 1}]}
    ]
    merged = dc.deep_merge_configs(configs)
    assert merged == {"pools": [
        {"pool_name": "pool1", "param1": 1},
        {"pool_name": "pool2", "param1": 1},
        {"pool_name": "pool3", "param1": 1},
        {"pool_name": "pool4", "param1": 1}
    ]}

test_deep_merge_configs()
