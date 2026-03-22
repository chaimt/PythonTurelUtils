Do the following tasks:

* Run `pre-commit run` to validate all staged files meet standards. Fix any issues before proceeding.

* If no branch exists, create one using the naming convention:
   - `bug/<short-description>` for bug fixes
   - `feature/<short-description>` for new features

* Branch name must not have special chars like spaces or .

* Stage all relevant changes and commit with a descriptive message following conventional commit format (e.g., `feat:`, `fix:`, `chore:`).

* Push changes to the remote branch.

* Create a new PR in GitHub (or update the existing one if it already exists):
   - Set a clear, descriptive title
   - Update the description summarizing the current changes in the branch

* If a Jira ticket is associated with this work:
   - Add the Jira ticket link to the PR description
   - If no Jira ticket is present in the PR or provided, ask the user for the Jira ticket link before finalizing
