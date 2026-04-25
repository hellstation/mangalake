from etl.transform import manga_transform as mt


def test_extract_title_from_attributes_preferred_language():
    item = {
        "attributes": {
            "title": {
                "ru": "Тестовая манга",
                "en": "Test Manga",
            }
        }
    }

    assert mt._extract_title(item) == "Test Manga"


def test_extract_tags_from_nested_attributes_name():
    item = {
        "attributes": {
            "tags": [
                {"attributes": {"name": {"en": "Action"}}},
                {"attributes": {"name": {"ru": "Комедия"}}},
            ]
        }
    }

    assert mt._extract_tags(item) == "Action, Комедия"


def test_transform_latest_to_df_builds_expected_columns(monkeypatch):
    raw_items = [
        {
            "id": "manga-1",
            "attributes": {
                "title": {"en": "One Piece"},
                "status": "ongoing",
                "lastChapter": "1110",
                "year": "1997",
                "updatedAt": "2025-01-01T00:00:00Z",
                "tags": [{"attributes": {"name": {"en": "Adventure"}}}],
            },
        }
    ]

    def fake_loader(_prefix):
        for item in raw_items:
            yield item

    monkeypatch.setattr(mt, "_load_raw_records", fake_loader)

    df = mt.transform_latest_to_df("2025-01-01")

    assert list(df.columns) == [
        "MANGA_ID",
        "TITLE",
        "STATUS",
        "LAST_CHAPTER",
        "YEAR",
        "TAGS",
        "UPDATED_AT",
    ]
    assert df.shape[0] == 1
    row = df.iloc[0].to_dict()
    assert row["MANGA_ID"] == "manga-1"
    assert row["TITLE"] == "One Piece"
    assert row["STATUS"] == "ongoing"
    assert row["LAST_CHAPTER"] == "1110"
    assert row["YEAR"] == 1997
    assert row["TAGS"] == "Adventure"
    assert row["UPDATED_AT"] == "2025-01-01T00:00:00Z"
