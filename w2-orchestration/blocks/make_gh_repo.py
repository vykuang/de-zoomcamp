#!/usr/bin/env python
# coding: utf-8
"""
Create a github repo block from code
Note that these blocks need to be already registered in the CLI via

prefect register -m prefect_github

"""

from prefect_github.repository import GitHubRepository

repo_blk = GitHubRepository(
    repository_url="https://github.com/vykuang/de-zoomcamp",
    reference="prefect",
)
repo_blk.save("de-zoom-gh", overwrite=True)
