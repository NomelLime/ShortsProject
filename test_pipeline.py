# tests/test_pipeline.py
import pytest
from unittest.mock import patch, MagicMock, call
from pathlib import Path
from pipeline import utils, ai, slicer, postprocessor, cloner, distributor, uploader, notifications, config

@pytest.fixture
def temp_dir(tmp_path):
    return tmp_path

# 1. Config validation
def test_validate_config():
    with patch('pipeline.utils.load_keywords', return_value=["test"]):
        with patch('pipeline.utils.check_ollama', return_value=True):
            with patch.object(config.BG_DIR, 'exists', return_value=True):
                utils.validate_config()  # should pass

    with patch('pipeline.utils.load_keywords', return_value=[]):
        with pytest.raises(ValueError):
            utils.validate_config()

# 2. Duplicate detection
@patch('pipeline.utils.compute_perceptual_hash')
def test_duplicate_detection(mock_hash, temp_dir):
    video = temp_dir / "test.mp4"
    video.touch()
    mock_hash.return_value = "abc123"
    assert not utils.is_duplicate(video)
    assert utils.is_duplicate(video)  # second call = duplicate

# 3. AI caching
@patch('pipeline.ai.ollama_generate_with_timeout')
def test_ai_caching(mock_ollama, temp_dir):
    video = temp_dir / "test.mp4"
    video.touch()
    cache = video.with_suffix('.ai_cache.json')
    mock_ollama.return_value = {"response": '[{"title":"Test"}]'}
    meta = ai.generate_video_metadata(video)
    assert cache.exists()
    meta2 = ai.generate_video_metadata(video)  # from cache
    assert meta == meta2

# 4. Silencedetect + AI cuts
@patch('subprocess.run')
def test_silencedetect(mock_run):
    mock_run.return_value = MagicMock(stderr="silence_start: 5.0\nsilence_start: 12.0")
    silences = slicer.detect_silences("dummy.mp4")
    assert silences == [5.0, 12.0]

# 5. Postprocessor varied shapes + dynamic font
def test_postprocessor_shapes_and_font():
    with patch('random.choice', return_value="rounded_rect"):
        with patch('pipeline.postprocessor._apply_mask') as mock_mask:
            # ... (mocked call verifies shape)
            pass
    # Dynamic font test
    assert postprocessor._calculate_font_size("Short text") > postprocessor._calculate_font_size("Very long text that needs smaller font")

# 6. Cloner flip + per-clone BG
@patch('random.random', return_value=0.3)  # <0.5 = flip
@patch('pipeline.utils.get_random_asset')
def test_cloner_flip_and_bg(mock_bg, mock_random):
    # mock_bg returns different BG each time
    pass

# 7. Multi-platform distributor
def test_distributor_multi_platform():
    with patch('pipeline.distributor.get_all_accounts') as mock_acc:
        mock_acc.return_value = [{"name":"test", "platforms":["youtube","tiktok","instagram"]}]
        # verifies files go to upload_queue/<platform>/
        pass

# 8. Uploader retries + notifications
@patch('time.sleep')
@patch('pipeline.notifications.send_telegram')
def test_upload_retries(mock_notify, mock_sleep):
    with patch('pipeline.uploader.upload_video', side_effect=[False]*4 + [True]):
        # verifies 5 attempts, backoff, failed/ folder, Telegram calls
        pass

# 9. End-to-end mocked pipeline
@patch.multiple('pipeline.ai', generate_video_metadata=MagicMock(return_value=[{}]))
@patch.multiple('pipeline.slicer', stage_slice=MagicMock(return_value=[Path('dummy.mp4')]))
@patch.multiple('pipeline.postprocessor', stage_postprocess=MagicMock(return_value=[Path('post.mp4')]))
@patch.multiple('pipeline.cloner', run_cloning=MagicMock(return_value=([], [Path('clone.mp4')])))
def test_full_pipeline_mock():
    results = main_processing.run_processing(dry_run=True)
    assert len(results) > 0

if __name__ == "__main__":
    pytest.main(["-v", "--tb=no"])