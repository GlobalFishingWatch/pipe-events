import pytest

def pytest_addoption(parser):
    parser.addoption("--travis", action="store_true", 
        help="These tests are being run under Travis")
