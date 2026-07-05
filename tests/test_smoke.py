import os


def test_readme_exists():
    assert os.path.isfile("README.md")


def test_gitignore_exists():
    assert os.path.isfile(".gitignore")
