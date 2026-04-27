import logging
import pytest
from unittest.mock import MagicMock, patch
from flowstash.observability.logging import logger
from flowstash.context import integration_context, get_context
from flowstash.observability.ingestion import set_observability_config, enqueue_log_event
from flowstash.config.observability_config import ObservabilityConfig, LoggingConfig

def test_logger_outside_context():
    """Outside context, should passthrough to python logging."""
    with patch.object(logger._python_logger, 'log') as mock_log:
        logger.info("Test message %s", "arg", extra={"foo": "bar"}, custom="attr")
        mock_log.assert_called_once()
        args, kwargs = mock_log.call_args
        assert args[0] == logging.INFO
        assert args[1] == "Test message %s"
        assert args[2] == "arg"
        assert kwargs["extra"] == {"foo": "bar"}
        assert kwargs["custom"] == "attr"

def test_logger_inside_context():
    """Inside context, should enqueue to observability and NOT passthrough (as per instructions)."""
    with integration_context(integration="test-int"):
        with patch("flowstash.observability.logging.enqueue_log_event") as mock_enqueue:
            with patch.object(logger._python_logger, 'log') as mock_log:
                logger.info("Test message %s", "arg", extra={"foo": "bar"}, custom="attr")
                
                # Should capture
                mock_enqueue.assert_called_once()
                _, k = mock_enqueue.call_args
                assert k["logger_name"] == "flowstash.user"
                assert k["levelno"] == logging.INFO
                assert k["message"] == "Test message arg"
                assert k["attrs"] == {"foo": "bar", "custom": "attr"}
                assert k["exc_info"] is False
                
                # Should NOT passthrough if we follow "otherwise" strictly
                mock_log.assert_not_called()

def test_logger_exception():
    """logger.exception should behave like ERROR with exc_info=True."""
    with integration_context(integration="test-int"):
        with patch("flowstash.observability.logging.enqueue_log_event") as mock_enqueue:
            try:
                raise ValueError("Boom")
            except ValueError:
                logger.exception("Oops")
            
            mock_enqueue.assert_called_once()
            _, k = mock_enqueue.call_args
            assert k["levelno"] == logging.ERROR
            assert k["exc_info"] is True

def test_enqueue_log_event_filtering():
    """Verify filtering logic in enqueue_log_event."""
    # Mock store and async manager to verify calls
    with patch("flowstash.observability.ingestion.get_events_store") as mock_get_store, \
         patch("flowstash.observability.ingestion.AsyncManager") as mock_manager:
        
        mock_store = MagicMock()
        mock_get_store.return_value = mock_store
        # Make it sync for testing
        mock_manager.get_instance.return_value.execute_fire_and_forget.side_effect = lambda f, *a, **k: f(*a, **k)
        
        # Test 1: Disabled
        set_observability_config(ObservabilityConfig(logging=LoggingConfig(enabled=False)))
        with integration_context(integration="test-int"):
            enqueue_log_event("any", logging.INFO, "msg")
        mock_store.write_log.assert_not_called()
        
        # Test 2: Min Level
        set_observability_config(ObservabilityConfig(logging=LoggingConfig(min_level="ERROR")))
        with integration_context(integration="test-int"):
            enqueue_log_event("any", logging.INFO, "msg")
            enqueue_log_event("any", logging.ERROR, "msg")
        assert mock_store.write_log.call_count == 1
        
        # Test 3: Exclude prefixes
        mock_store.reset_mock()
        set_observability_config(ObservabilityConfig(logging=LoggingConfig(exclude_prefixes=["foo"])))
        with integration_context(integration="test-int"):
            enqueue_log_event("foo.bar", logging.ERROR, "msg")
            enqueue_log_event("baz", logging.ERROR, "msg")
        assert mock_store.write_log.call_count == 1
        
        # Test 4: Include prefixes
        mock_store.reset_mock()
        set_observability_config(ObservabilityConfig(logging=LoggingConfig(include_prefixes=["allowed"])))
        with integration_context(integration="test-int"):
            enqueue_log_event("allowed.one", logging.ERROR, "msg")
            enqueue_log_event("disallowed", logging.ERROR, "msg")
        assert mock_store.write_log.call_count == 1

def test_recursion_safety():
    """Should not capture logs from flowstash.observability."""
    with patch("flowstash.observability.ingestion.get_events_store") as mock_get_store:
        mock_store = MagicMock()
        mock_get_store.return_value = mock_store
        
        set_observability_config(ObservabilityConfig())
        with integration_context(integration="test-int"):
            enqueue_log_event("flowstash.observability.someting", logging.ERROR, "msg")
        
        mock_store.write_log.assert_not_called()

def test_swallow_exceptions():
    """Ingestion should never raise."""
    with patch("flowstash.observability.ingestion.current_context", side_effect=Exception("Failed")):
        # Should not raise
        enqueue_log_event("foo", logging.INFO, "msg")

def test_filter_fn_resolution():
    """Verify filter_fn can be resolved and called."""
    # Define a filter function in this module
    def my_filter(record, ctx):
        return "Allowed" in record["message"]
    
    # We need to be able to resolve it by string path
    # For testing, we can patch _resolve_callable
    with patch("flowstash.observability.ingestion._resolve_callable", return_value=my_filter), \
         patch("flowstash.observability.ingestion.AsyncManager") as mock_manager:
        
        # Make it sync for testing
        mock_manager.get_instance.return_value.execute_fire_and_forget.side_effect = lambda f, *a, **k: f(*a, **k)

        with patch("flowstash.observability.ingestion.get_events_store") as mock_get_store:
            mock_store = MagicMock()
            mock_get_store.return_value = mock_store
            
            set_observability_config(ObservabilityConfig(logging=LoggingConfig(filter_fn="some.path")))
            with integration_context(integration="test-int"):
                enqueue_log_event("any", logging.INFO, "Allowed message")
                enqueue_log_event("any", logging.INFO, "Dropped message")
            
            assert mock_store.write_log.call_count == 1
