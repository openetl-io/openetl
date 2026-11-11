#!/usr/bin/env bash
# Backports a merged pull request to the release branches it's labelled for,
# as .github/workflows/backport.yml describes. It reads, from the environment:
#
#   PR       the pull request's number   TITLE    its title
#   COMMIT   its merge_commit_sha        COMMITS  how many commits it has
#   ACTION   closed, or labeled          LABEL    the label, when labeled
#   LABELS   its labels, as a JSON array
#
# It runs in a checkout with every branch fetched, with gh signed in with a
# token that can push and comment.
set -euo pipefail

repo=${GITHUB_REPOSITORY:-openetl-io/openetl}

# What merging put on the base branch, to cherry-pick: a merge commit, against
# its first parent; the pull request's commits, if they were rebased onto it;
# or the commit they were squashed into.
picks=("$COMMIT")
if [ "$(git rev-list --no-walk --parents "$COMMIT" | wc -w)" -gt 2 ]; then
  picks=(-m 1 "$COMMIT")
elif [ "$COMMITS" -gt 1 ]; then
  git fetch --quiet origin "refs/pull/$PR/head"
  patch() { git show "$1" | git patch-id --stable | cut -d' ' -f1; }
  head=$(patch FETCH_HEAD)
  if [ -n "$head" ] && [ "$head" = "$(patch "$COMMIT")" ]; then
    picks=("$COMMIT~$COMMITS..$COMMIT")
  fi
fi

comment() { gh pr comment "$PR" -R "$repo" --body "$1"; }

# What to run to backport by hand, when the cherry-pick conflicts.
by_hand() {
  printf '%s\n' \
    "This doesn't apply cleanly to \`$1\`. To backport it by hand:" \
    '' \
    '```sh' \
    'git fetch origin' \
    "git switch -c $2 origin/$1" \
    "git cherry-pick -x ${picks[*]}" \
    '# Resolve the conflicts, then: git cherry-pick --continue' \
    "git push -u origin $2" \
    "gh pr create --base $1 --fill" \
    '```'
}

if [ "$ACTION" = labeled ]; then
  labels=$LABEL
else
  labels=$(jq -r '.[]' <<< "$LABELS")
fi

while IFS= read -r label; do
  case $label in "backport "*) ;; *) continue ;; esac
  target=${label#backport }
  if ! [[ $target =~ ^release-[0-9]+\.[0-9]+$ ]]; then
    comment "\`$label\` names no release line's branch, such as \`release-0.1\`."
    continue
  fi
  if ! git rev-parse --verify --quiet "refs/remotes/origin/$target" > /dev/null; then
    comment "There's no branch \`$target\` to backport this to."
    continue
  fi
  branch=backport/$PR-$target
  if git rev-parse --verify --quiet "refs/remotes/origin/$branch" > /dev/null; then
    echo "$branch exists: backported already"
    continue
  fi

  git switch --quiet -c "$branch" "origin/$target"
  if ! git cherry-pick -x --empty=drop "${picks[@]}"; then
    git cherry-pick --abort
    comment "$(by_hand "$target" "$branch")"
  elif [ "$(git rev-list --count "origin/$target..HEAD")" = 0 ]; then
    echo "$target has this change already"
  else
    git push --quiet origin "$branch"
    if ! gh pr create -R "$repo" --base "$target" --head "$branch" --title "$TITLE ($target)" --body "Backports #$PR to \`$target\`."; then
      comment "Backported to \`$branch\`, which needs [a pull request](https://github.com/$repo/compare/$target...$branch?expand=1) against \`$target\`."
    fi
  fi
  git switch --quiet --detach "$COMMIT"
done <<< "$labels"
