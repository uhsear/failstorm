# failstorm

Hold the mails about a failure storm, so the one mail that matters is not lost in the rest.

A storage volume fills up at 02:00. A dozen scheduled exports that write to it start to fail. A
small cron script checks the jobs every hour and sends a mail whenever the set of failing jobs
changes. At 02:00 it names three jobs. At 03:00 it names a fourth, and sends another mail. At 04:00
it names a fifth. By morning the owner has ten mails that all say "disk full", each one a little
longer than the last. The owner stops reading them.

At 04:00 an unrelated report also failed, because a login token had expired. That failure is in
one of the ten mails, as one more line in a long list. Nobody reads it, and it stays broken for a
day.

The obvious fix is to hold the mails that repeat the cause of the incident. That fix has a
trap, and the trap is the reason this tool exists. The easy way to decide "this failure repeats
the cause" is to compare it with the main cause of everything that is failing now. Then a batch of
new failures can change the answer. Three jobs are failing on disk space and a mail has said so.
Four login failures arrive. Four is more than three, so "login" is now the main cause of the
open set. The four new failures match the main cause, so they count as more of the same, and they
are held. The new problem hides behind its own size. This is the majority flip.

failstorm compares a new failure with the causes the last mail listed, not with the open set. Four
login failures are a cause that mail did not list, so they go out at once, however many there are.
The self-test pins this case, and shows that the majority rule gets it wrong.

```
$ python failstorm.py --self-test
failstorm self-test: temporary files only, a stub on 127.0.0.1, no credentials
--------------------------------------------------------------------
...
PASS  an empty file is refused, a crashed producer writes one  <-- pinned defect
...
PASS  four new failures of another cause outnumber three, and are sent at once  <-- pinned defect
...
PASS  the rule that compares with the majority open now holds them, so this case can tell the two rules apart
...
PASS  a held failure that recovers is still reported once  <-- pinned defect
...
PASS  a state of text that is not JSON is treated as empty with a warning, never as silence  <-- pinned defect
...
PASS  a held run prints nothing, so cron mails nothing, yet saves the hold  <-- pinned defect
...
PASS  an empty failing file exits 4 and leaves the state alone  <-- pinned defect
...
PASS  the flag prefix --ap is refused with exit 64 and writes nothing  <-- pinned defect
...
PASS  a webhook that answers 500 exits 3 and leaves the state, so the next run sends again  <-- pinned defect
...
PASS  a redirect is a failed delivery, and is not followed as a GET  <-- pinned defect
...
PASS  an unexpected exception exits 70, not a code a wrapper reads as a finding  <-- pinned defect
...
PASS  the footer reports failures by count and by name, and exits 1  <-- pinned defect
...
--------------------------------------------------------------------
166 assertions, 0 failed
```

The full run prints all 166 assertions. Each `...` line above is where this block is cut.

## What already exists

Cron mails any output a job prints. The Debian
[cron(8)](https://manpages.debian.org/bookworm/cron/cron.8.en.html) page says that any output of
a command is mailed to the owner of the crontab, or to the `MAILTO` user. That is the mail
channel this tool uses, and it is why a run that has nothing to say must print nothing.

[cronic](https://github.com/justincase/cronic) is a small shell wrapper for one job. Its README
says it makes cron send a mail only when the job fails, and that it has no options. It fixes the
case of a job that prints on success. It wraps one job and, as far as that README shows, remembers
nothing between runs. A job that fails every hour still mails every hour. Use it for that case,
and use failstorm for the case where many jobs fail from one cause.

[Prometheus Alertmanager](https://prometheus.io/docs/alerting/latest/configuration/) does the
grouping well. `group_by` collects alerts into one notification. The `group_wait` text says that an
alert which resolves before `group_wait` has passed sends no notification. The `group_interval`
text says Alertmanager checks at each interval whether alerts fired or resolved, and sends if they
did. The `repeat_interval` text covers the repeat of an unchanged set. If the cause is a label and
you group by it, a new cause becomes a new group and the effect is close to this tool. If Prometheus
already runs, use it. Its documentation describes no rule that compares a new alert with the causes
the previous notification listed.

[Healthchecks](https://github.com/healthchecks/healthchecks) is described on GitHub as an
open-source cron job and background task monitoring service. It was not tested for this report.

A search of GitHub for the name `failstorm` found two unrelated repositories (a 2014 game project
and a 2018 test bench), and searches for a state-file hold on cron failure mails found no other
tool on the day it was run. That shows only that the search found nothing.

## Requirements

Python 3.9 or newer and nothing else. No third-party package, no database, no mail server.

The self-test prints the same 166 assertions, line for line, on Windows (Python 3.13.2), on
Python 3.9.25 and on Ubuntu (Python 3.12.3). `coverage run --branch failstorm.py --self-test`
reports 100 percent of lines and branches (785 statements, 154 branches).

```
git clone https://github.com/uhsear/failstorm.git
```

## Usage

The script that checks your jobs writes one failing job per line to a file, and failstorm decides
whether that run needs a mail.

```
python failstorm.py --self-test
python failstorm.py --state fs.json --failing now.txt
python failstorm.py --state fs.json --failing now.txt --cause "storage=no space|read-only"
python failstorm.py --state fs.json --failing now.txt --apply
python failstorm.py --state fs.json --failing - --webhook http://host:8080/hook --apply
```

A crontab line, run every hour. It assumes `check_jobs.sh` prints the failing file to stdout.
The `;` is deliberate: the check may exit non-zero when jobs fail, and the tool must still run.
A check that crashes leaves an empty file, which the tool refuses with exit 4.

```
0 * * * * /path/to/check_jobs.sh > /var/tmp/now.txt; python3 /path/to/failstorm.py --state /var/tmp/fs.json --failing /var/tmp/now.txt --cause "storage=no space" --apply
```

| Flag | Default | What it does |
|---|---|---|
| `--state` | none | The state file. Read every run. Written after a send or a hold. Required. |
| `--failing` | none | The failing file, or `-` for stdin. Required. |
| `--cause` | none | `CAUSE=REGEX`. Classifies the evidence text of each failure. May repeat. First match wins. |
| `--storm-min` | `3` | Failures open at which a mail starts an incident. `0` means never, so nothing is held. |
| `--digest-hours` | `6` | Hours a held failure waits after the incident mail. `0` holds nothing. |
| `--realert-hours` | `24` | Hours after which an unchanged set is mailed again. `0` never repeats. |
| `--max-list` | `25` | Names listed per section of a mail. The rest become "and N more". |
| `--webhook` | off | POST the notice as JSON to this http or https URL instead of printing it. |
| `--timeout` | `10` | Seconds to wait for the webhook. |
| `--now` | the clock | The time of this run as ISO 8601. For a replay. |
| `--apply` | off | Send the notice and write the state. Without it nothing is sent or written. |
| `--self-test` | off | Run the assertions and exit. Other flags are ignored. |

Every flag must be spelled in full. The parser sets `allow_abbrev=False`, so `--ap` is refused with
exit 64 instead of being read as `--apply`, and `--stat` is refused instead of being read as
`--state`. Python's [argparse](https://docs.python.org/3/library/argparse.html) allows
abbreviations by default. A script that sends mail and writes a state file should not guess.

## The failing file

Each line is a job name, or a job name, a tab and some evidence text, such as the last error line
of the job's log. A line that starts with `#` is a comment. A clean run writes the single line
`#ok`.

A file with no failures and no `#ok` line is refused with exit 4. A check script that crashed
before it wrote anything leaves exactly that file. If the file were read as "nothing is failing",
the tool would send a false all clear and close the incident.

A name that appears twice is refused. The tool does not merge two lines about one job, because the
second line would hide the first.

## How it decides

The state file holds the set of failures the last mail named, the causes that mail listed, the time
of the last incident mail, and the first time each failure was seen. Each run compares the failing
file with it.

| What changed | What happens |
|---|---|
| Nothing failing, nothing announced | Nothing. No output. |
| The same set, less than `--realert-hours` since the last mail | Nothing. |
| The same set, `--realert-hours` or more | A mail, "still open". |
| Failures appear, fewer than `--storm-min` open | A mail at once. |
| Failures appear, no incident mail yet | A mail at once. It starts the incident. |
| Only new failures, causes all listed by the incident mail or unknown, before the digest is due | Held. Nothing is printed. The state records what waits. |
| The same, and the digest is due | A digest mail. |
| A new failure with a cause the last mail did not list | A mail at once. |
| Any failure recovers | A mail at once. |
| A held failure recovered before its digest | A mail at once, under "FAILED AND RECOVERED BEFORE THIS MAIL". |
| Every failure recovers | A mail, "all clear". It closes the incident. |

The cause in a subject line is the cause held by more than half of the open failures, or "no single
cause". That label is only for the subject. The hold rule never reads it.

A subject line also names a cause that is new since the last incident mail, as in
`failstorm: 5 failing, shared cause: storage, new cause: auth`.

## A run you can repeat

Six files, six cron ticks, and the real output. The `--now` flag stands in for the clock.
`t1.txt` has three storage failures. `t2.txt` adds `export-d`. `t3.txt` adds a login failure
`report-a`. `t4.txt` adds `export-e`. `t5.txt` is the same set as `t4.txt`, ten hours after the
first mail. `t6.txt` is `#ok`.

```
$ python failstorm.py --state state.json --failing t1.txt --cause "storage=no space|read-only" --cause "auth=token|401" --now 2026-03-03T02:00:00Z --apply
Subject: failstorm: 3 failing, shared cause: storage

New failures.
time: 2026-03-03T02:00:00Z
open: 3, the earliest since 2026-03-03T02:00:00Z
causes: storage 3
NEW (3):
  export-a [storage] since 2026-03-03T02:00:00Z: OSError: no space left on device
  export-b [storage] since 2026-03-03T02:00:00Z: OSError: no space left on device
  export-c [storage] since 2026-03-03T02:00:00Z: OSError: no space left on device
More failures of a cause named above wait for a digest at most every 6 hours. A new cause or a recovery is mailed at once.
```

At 03:00 the fourth storage failure is held, and the run prints nothing and exits 0. At 04:00 the
login failure is a cause the 02:00 mail did not list:

```
Subject: failstorm: 5 failing, shared cause: storage, new cause: auth

The set of failures changed.
time: 2026-03-03T04:00:00Z
open: 5, the earliest since 2026-03-03T02:00:00Z
causes: auth 1, storage 4
NEW (2):
  export-d [storage] since 2026-03-03T03:00:00Z: OSError: no space left on device
  report-a [auth] since 2026-03-03T04:00:00Z: HTTP 401: token expired
STILL OPEN (3):
  export-a [storage] since 2026-03-03T02:00:00Z: OSError: no space left on device
  export-b [storage] since 2026-03-03T02:00:00Z: OSError: no space left on device
  export-c [storage] since 2026-03-03T02:00:00Z: OSError: no space left on device
More failures of a cause named above wait for a digest at most every 6 hours. A new cause or a recovery is mailed at once.
```

At 05:00 `export-e` is held, with no output. At 10:00 the digest is due, six hours after the 04:00
mail:

```
Subject: failstorm digest: 6 failing, shared cause: storage

Digest of the failures held since the last mail.
time: 2026-03-03T10:00:00Z
open: 6, the earliest since 2026-03-03T02:00:00Z
causes: auth 1, storage 5
NEW (1):
  export-e [storage] since 2026-03-03T05:00:00Z: OSError: read-only file system
STILL OPEN (5):
  export-a [storage] since 2026-03-03T02:00:00Z: OSError: no space left on device
  export-b [storage] since 2026-03-03T02:00:00Z: OSError: no space left on device
  export-c [storage] since 2026-03-03T02:00:00Z: OSError: no space left on device
  export-d [storage] since 2026-03-03T03:00:00Z: OSError: no space left on device
  report-a [auth] since 2026-03-03T04:00:00Z: HTTP 401: token expired
More failures of a cause named above wait for a digest at most every 6 hours. A new cause or a recovery is mailed at once.
```

At 11:00 `#ok` produces the all clear:

```
Subject: failstorm: all clear

Every failure has recovered.
time: 2026-03-03T11:00:00Z
RECOVERED (6):
  export-a
  export-b
  export-c
  export-d
  export-e
  report-a
```

The same command without `--apply` prints the decision and the mail, and writes nothing:

```
$ python failstorm.py --state state.json --failing t1.txt --cause "storage=no space" --now 2026-03-03T12:00:00Z
failstorm check only: SEND (first) by stdout, which cron mails
Subject: failstorm: 3 failing, shared cause: storage

New failures.
time: 2026-03-03T12:00:00Z
open: 3, the earliest since 2026-03-03T12:00:00Z
causes: storage 3
NEW (3):
  export-a [storage] since 2026-03-03T12:00:00Z: OSError: no space left on device
  export-b [storage] since 2026-03-03T12:00:00Z: OSError: no space left on device
  export-c [storage] since 2026-03-03T12:00:00Z: OSError: no space left on device
More failures of a cause named above wait for a digest at most every 6 hours. A new cause or a recovery is mailed at once.

Nothing was sent and nothing was written. Re-run with --apply.
```

## Delivery

Without `--webhook`, a run that sends prints `Subject: ...`, a blank line and the body on stdout.
Under cron, the daemon mails that to the crontab owner. A run that holds or has nothing to say
prints nothing on either stream, so cron mails nothing. The self-test proves this on a real
process: both streams are empty bytes.

With `--webhook`, the tool POSTs `{"subject", "body", "reason", "open"}` as JSON and prints nothing
on success. The self-test exercises the webhook against a stub HTTP server on `127.0.0.1`.

- A 4xx or 5xx answer, a refused connection and a timeout each exit 3.
- On exit 3 the state is not advanced. The next run sends the same notice again.
- A redirect is a failed delivery. The standard library's default opener follows a redirect on a
  POST as a GET with the body gone, and that GET can answer 200. The self-test shows this with a
  control request, then shows the tool does not do it. The Python
  [urllib.request](https://docs.python.org/3/library/urllib.request.html) docs describe
  `redirect_request` as the hook that decides whether a redirect happens.
- No message names the URL. A webhook URL often carries its secret in the path. The self-test
  runs with a marker in the path and searches stdout, stderr and the state file for it.
- If the notice was delivered and the state then cannot be written, the tool exits 2 and says the
  notice went out, because the next run will send it again.

SMTP is not offered. The standard library's `smtpd` module was removed in Python 3.12, per the
[What's New](https://docs.python.org/3/whatsnew/3.12.html) page, so a mail server cannot be
stubbed offline with the standard library on every supported version. Cron's own mail covers it.

## Exit codes

| Code | Meaning |
|---|---|
| 0 | The decision was made and carried out, or only shown. Held and quiet runs are 0. |
| 2 | A file could not be read or written. |
| 3 | The webhook did not take the notice. The state was not advanced. |
| 4 | The failing file had no failures and no `#ok` line. Nothing was changed. |
| 64 | Usage error, including a refused abbreviation. |
| 70 | An unexpected crash. This is a fault in the tool, not a finding. |

## Limits

- The state file has one writer. There is no lock. Two overlapping runs can both read the old
  state and both send. Keep the interval longer than the run, or wrap the call in `flock`.
- A new failure with no recognised cause is held, like more of the same, up to `--digest-hours`.
  If your `--cause` rules are thin, most failures will be unclassified and held. Write rules for
  the causes you care about, and keep `--digest-hours` short enough to live with.
- Causes are regular expressions over the evidence text. A wrong rule gives a wrong hold.
- A cron daemon that is not running, or a check script that never runs, produces silence. This
  tool cannot see that. Pair it with a separate monitor that expects a regular check-in, such as Healthchecks (not
  tested here).
- The tool was never run under a real cron daemon in this test. The cron behaviour (any output is
  mailed) is cited from its man page. What was tested is that a real process prints nothing when
  it holds.
- An unreadable state file is treated as empty, with a warning on stderr. The next run may
  announce open failures again. An extra mail is a better failure than a silent one.
- All times are UTC. A recorded incident time in the future never holds, so a clock set back
  cannot mute the tool.
- Coverage was measured on Windows only. The Linux and Python 3.9 runs ran the same self-test and
  printed the same 166 assertions.

## Related

Other single-file tools in this portfolio that pair with this one:

- [stalled](https://github.com/uhsear/stalled) - count the days since the last success of a
  scheduled notebook or webhook and say so when the count is too high. It answers "did the job stop"; this tool
  answers "how many mails should the failure cost".
- [taskpulse](https://github.com/uhsear/taskpulse) - audit Windows scheduled tasks for silent
  failures and for the configuration that causes them.
- [logsift](https://github.com/uhsear/logsift) - mine a scheduled job's own log files into a
  metric series, and refuse the number that is really a timestamp.
- [jobharness](https://github.com/uhsear/jobharness) - logging, retry, resume and a run lock for an
  unattended script.
