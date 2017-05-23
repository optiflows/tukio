# Contributing to Tukio

Thanks for considering contributing to Tukio! This guide contains information about how to report new issues and how to hack properly on the library.

## Topics

* [Code of conduct](#code-of-conduct)
* [Issues and bug reports](#issues-and-bug-reports)
* [New feature proposals](#new-feature-proposals)
* [Patches submission](#patches-submission)
* [Reach out](#reach-out)

## Code of conduct

(Adapated from [TODO Group's Open Source of Conduct](http://todogroup.org/opencodeofconduct/))

This code of conduct outlines our expectations for participants within the Surycat community. We are committed to providing a welcoming and inspiring community for all and expect our code of conduct to be honored. Anyone who violates this code of conduct may be banned from the community.

Our open source community strives to:

* **Be friendly and patient.**
* **Be welcoming**: We strive to be a community that welcomes and supports people of all backgrounds and identities. This includes, but is not limited to members of any race, ethnicity, culture, national origin, colour, immigration status, social and economic class, educational level, sex, sexual orientation, gender identity and expression, age, size, family status, political belief, religion, and mental and physical ability.
* **Be considerate**: Your work will be used by other people, and you in turn will depend on the work of others. Any decision you take will affect users and colleagues, and you should take those consequences into account when making decisions.
* **Try to understand why we disagree**: Disagreements, both social and technical, happen all the time. It is important that we resolve disagreements and differing views constructively. Remember that we’re different.
* **Be respectful**: Not all of us will agree all the time, but disagreement is no excuse for poor behavior and poor manners.
* **Be careful in the words that we choose**: we are a community of professionals, and we conduct ourselves professionally. Be kind to others. Do not insult or put down other participants. Harassment and other exclusionary behavior aren’t acceptable.

If you experience or witness unacceptable behavior—or have any other concerns—please report it by contacting us via <abuse@surycat.com>. All reports will be handled with discretion.

## Issues and bug reports

Found a bug? Great! Building quality software is what we are aiming to, so do not hesitate to open an issue.

First, before you submit a new issue, search to archive to avoid any duplicate work. If your issue appears to be a bug and has not been reported yet, a thorough, cleanly written report will increase the likelihood of your issue being picked up by a contributor:

* Describe the issue using a clear and short sentence.
* Include Tukio's version and the version of the Python interpreter used.
* A no-brainer step by step bug reproduction guide
* Attach a traceback if possible.
* (optional) Suggest a fix, if you can't fix it by yourself you can still point to the root causes (specific commits) or even find similar bug that have been previously reported.

## New feature proposals

Features can be requested using GitHub's issue ticketing system. Have an awesome idea for Tukio? Submit an issue, if you'd like to implement the said feature, link your work to the issue so that everyone can have a look at it.

* For a **major feature**, or big changes that can potentially break things, first raise an issue and describe it thoroughly (motivation behind it, code snippet, API description, etc.) so that it can be discussed.
* **Smaller feature** can be hacked on and directly submitted as PR.

## Patches submission

Tukio follows the classic GitHub's zen "fork-branch-PR" regarding patch submissions.

1. [Fork](https://help.github.com/articles/fork-a-repo/) Tukio

    Create a "copy" of this repository under your own username by using the fork feature, this will allow you to experiment your hacks without altering the original project.

2. [Create a branch](https://help.github.com/articles/creating-and-deleting-branches-within-your-repository/)

    When creating a branch, please do include the issue number that your code will address and a sensible name e.g. if an issue has been raised with the index `42` regarding fixing unit tests, the following branch names can be considered as valid:

    * johndoe/issue-42/fixing-unit-tests
    * 42-fixing-tests


3. Work on the bug resolution or new feature implementation

    We recommend creating a [virtualenv](https://pypi.python.org/pypi/virtualenv) beforehand when hacking on Tukio:

    ```bash
    surycat$ virtualenv ~/projects/tukio
    surycat$ source ~/projects/tukio/bin/activate
    ```

    Then installing the library using the "[editable install](https://pip.pypa.io/en/stable/reference/pip_install/#editable-installs)" feature of `pip`:

    ```bash
    (tukio) surycat$ git clone git@github.com:optiflows/tukio.git ~/sources/tukio
    (tukio) surycat$ pip install -e ~/sources/tukio
    ```

    To maintain a coherent coding style, any new code must be as [PEP 8](https://www.python.org/dev/peps/pep-0008/) compliant as possible.

4. Fix and/or write new unit tests

    All existing tests must pass in order for a PR to be merged. If your PR implements a new feature, please write new unit tests.

    Unit tests are using [nose](https://nose.readthedocs.org/en/latest/) and can be launched by invoking `nosetests` inside a properly set up virtualenv.

5. Tidy up your commits

    Before you make a PR, squash your commits into logical units of work using `git rebase -i` and `git push -f`. A logical unit of work being a coherent set of code changes that should be reviewed together.

6. Create the PR

    When all the above steps have been achieved, you're good to go! [Create a pull request](https://help.github.com/articles/creating-a-pull-request/) in which you'll reference the issue number.
