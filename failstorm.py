#!/usr/bin/env python
"""Hold the mails about a failure storm, so the one that matters is not lost.

A cron script that checks a list of jobs and mails on every change turns one
root cause into a mail per tick. This tool sits between that check and the
mailbox. It reads the failing names, compares them with the state it saved on
the last mailed run, and decides: send now, hold for a digest, or stay quiet.

The hold rule. Once a mail has announced an incident (at least --storm-min
failures open), a run whose only change is NEW failures whose cause the last
mail already listed, or whose cause is unknown, waits for the next digest,
--digest-hours after that mail. A recovery, a new cause, a first incident mail
and a failure that comes back after a quiet spell are sent at once.

The pinned defect, the majority flip. The cause of a new failure must be
compared with the causes the LAST MAIL ANNOUNCED. Compared with the majority of
the failures open now, a batch of new failures of another kind can outnumber
the old cause, become the majority, look like "more of the same" and be held.

    python failstorm.py --self-test
    python failstorm.py --state s.json --failing now.txt
    python failstorm.py --state s.json --failing now.txt --cause disk=no space
    python failstorm.py --state s.json --failing now.txt --apply
    python failstorm.py --state s.json --failing - --webhook URL --apply
    FS_HOOK=URL python failstorm.py --state s.json --failing now.txt --webhook-env FS_HOOK --apply

The failing file has one failure per line: NAME, or NAME, a tab and evidence
text. A line whose first visible character is # is a comment. A clean run
writes the single line #ok. A file with neither is refused,
because a producer that crashed writes exactly that, and reading it as "all
recovered" would send a false all-clear. --cause CAUSE=REGEX classifies the
evidence text, first match wins. Nothing is sent or written without --apply.
With --apply a run that sends prints the mail on stdout, which cron mails to
its owner, and a run that holds or has nothing to say prints nothing.

Exit codes: 0 the decision was made and carried out (or only shown), 2 a file
could not be read or written, 3 the webhook did not take the notice and the
state was not advanced, 4 the failing file held no failures and no #ok line,
64 usage error or a malformed failing file (a repeated name, #ok beside
failures, UTF-16 text), 70 an unexpected crash, which is never a finding.
"""

from __future__ import print_function

import argparse
import contextlib
import datetime
import importlib.util
import io
import json
import math
import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, HTTPServer

# Delivery is stdout (which cron mails) or an HTTP webhook. SMTP is not
# offered: cron already mails stdout, and a second mail path would add a
# credential to guard and one more way to send twice.

STATE_VERSION = 1
DEFAULT_STORM_MIN = 3
DEFAULT_DIGEST_HOURS = 6.0
DEFAULT_REALERT_HOURS = 24.0
DEFAULT_MAX_LIST = 25
DEFAULT_TIMEOUT = 10.0
EVIDENCE_WIDTH = 80
MAX_NUMBER = 1e6          # largest --digest-hours, --realert-hours, --timeout
YEAR_RANGE = (1900, 9000)  # times outside this overflow datetime arithmetic
SUBJECT_TAG = "failstorm"

REASON_TEXT = {
    "first": "New failures.",
    "changed": "The set of failures changed.",
    "recovered": "Every failure has recovered.",
    "digest": "Digest of the failures held since the last mail.",
    "realert": "These failures are still open.",
}


class EmptyInput(ValueError):
    """The failing file held no failure and no #ok line."""


class DeliveryError(Exception):
    """The webhook did not take the notice."""


# ----------------------------------------------------------------- time

def parse_time(text):
    """An ISO 8601 time as an aware UTC datetime, or None when it is not one
    or lies outside YEAR_RANGE. A time with no offset is read as UTC."""
    if not isinstance(text, str):
        return None
    text = text.strip()
    if text[-1:] in ("Z", "z"):
        text = text[:-1] + "+00:00"
    try:
        value = datetime.datetime.fromisoformat(text)
        if value.tzinfo is None:
            value = value.replace(tzinfo=datetime.timezone.utc)
        value = value.astimezone(datetime.timezone.utc)
    except (ValueError, OverflowError):
        return None
    if not YEAR_RANGE[0] <= value.year <= YEAR_RANGE[1]:
        return None
    return value


def stamp(moment):
    return moment.astimezone(datetime.timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------- input

def parse_failures(text):
    """{name: evidence} from the failing file's text.

    Raises EmptyInput for a file with no failure and no #ok line, and
    ValueError for a duplicated name, a line with no name, or #ok beside
    failures. A name repeated is refused, not merged, because the second
    line's evidence would silently replace or lose the first's."""
    failures = {}
    saw_ok = False
    for number, line in enumerate(text.split("\n"), 1):
        line = line.rstrip("\r")
        if not line.strip():
            continue
        if line.lstrip().startswith("#"):
            saw_ok = saw_ok or line.strip().lower() == "#ok"
            continue
        name, _, evidence = line.partition("\t")
        name = name.strip()
        if not name:
            raise ValueError("line %d has evidence but no name" % number)
        if name in failures:
            raise ValueError("line %d repeats the name %r" % (number, name))
        failures[name] = evidence.strip()
    if failures and saw_ok:
        raise ValueError("the file has an #ok line and failures; one of the "
                         "two is wrong")
    if not failures and not saw_ok:
        raise EmptyInput("the failing file has no failures and no #ok line. "
                         "A producer that crashed writes an empty file, and "
                         "an empty file must not read as all clear.")
    return failures


def parse_rules(specs):
    """[(cause, compiled regex)] from CAUSE=REGEX strings."""
    rules = []
    for spec in specs:
        cause, sep, pattern = spec.partition("=")
        cause = cause.strip()
        if not sep or not cause or not pattern:
            raise ValueError("--cause needs CAUSE=REGEX, got %r" % spec)
        try:
            rules.append((cause, re.compile(pattern, re.IGNORECASE)))
        except re.error as exc:
            raise ValueError("--cause %s: bad regex (%s)" % (cause, exc))
    return rules


def classify(evidence, rules):
    """The cause of the first rule that matches the evidence, else ''."""
    for cause, pattern in rules:
        if pattern.search(evidence):
            return cause
    return ""


def shared_cause(causes):
    """(label, counts) for {name: cause}. The label is the cause held by a
    strict majority of the names, else ''. The blank cause never wins."""
    counts = {}
    for cause in causes.values():
        counts[cause] = counts.get(cause, 0) + 1
    named = sorted((c for c in counts if c), key=lambda c: (-counts[c], c))
    top = named[0] if named else ""
    label = top if top and counts[top] * 2 > len(causes) else ""
    return label, counts


# ---------------------------------------------------------------- state

def new_state():
    return {"version": STATE_VERSION, "signature": [], "announced": [],
            "incident_utc": None, "pending": [], "first_seen": {},
            "last_sent_utc": None}


def check_state(raw):
    """raw when it is a well-formed state, else ValueError."""
    if not isinstance(raw, dict) or raw.get("version") != STATE_VERSION:
        raise ValueError("not a failstorm state of version %d" % STATE_VERSION)
    for key in ("signature", "announced", "pending"):
        value = raw.get(key)
        if not isinstance(value, list) or not all(
                isinstance(item, str) for item in value):
            raise ValueError("state field %s is not a list of strings" % key)
    seen = raw.get("first_seen")
    if not isinstance(seen, dict) or not all(
            isinstance(k, str) and isinstance(v, str)
            for k, v in seen.items()):
        raise ValueError("state field first_seen is not a map of strings")
    for key in ("incident_utc", "last_sent_utc"):
        if key not in raw:
            raise ValueError("state field %s is missing" % key)
        value = raw[key]
        if value is not None and parse_time(value) is None:
            raise ValueError("state field %s is not a time" % key)
    return raw


def load_state(path):
    """(state, warning). A missing file is a fresh state. An unusable file is
    a fresh state too, with a warning: the next mail re-announces what is
    open, and an extra mail is a better failure than a silent one."""
    if not os.path.exists(path):
        return new_state(), None
    with open(path, "r", encoding="utf-8-sig", errors="replace") as handle:
        text = handle.read()
    try:
        return check_state(json.loads(text)), None
    except (ValueError, RecursionError) as exc:
        return new_state(), ("state file %s is unusable (%s). Treating it as "
                             "empty, so this run may announce failures "
                             "again." % (path, exc))


def save_state(path, state):
    """Write the state atomically: a temp file in the same folder, then a
    rename, so a crash never leaves half a state for the next run to trust."""
    folder = os.path.dirname(os.path.abspath(path))
    fd, temp = tempfile.mkstemp(prefix=".failstorm-", suffix=".tmp",
                                dir=folder)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(state, handle, indent=2, sort_keys=True)
            handle.write("\n")
        os.replace(temp, path)
    except Exception:
        with contextlib.suppress(OSError):
            os.unlink(temp)
        raise


# ------------------------------------------------------------- decision

class Decision(object):
    """What to do. state is what to save afterwards, None for no write."""

    def __init__(self, send, reason, subject="", body="", state=None,
                 due=None):
        self.send = send
        self.reason = reason
        self.subject = subject
        self.body = body
        self.state = state
        self.due = due


def _section(lines, title, items, max_list):
    if not items:
        return
    lines.append("%s (%d):" % (title, len(items)))
    lines.extend("  " + text for text in items[:max_list])
    if len(items) > max_list:
        lines.append("  ... and %d more" % (len(items) - max_list))


def _describe(name, failures, first_seen):
    cause, evidence = failures[name]
    text = "%s [%s] since %s" % (name, cause or "unclassified",
                                 first_seen[name])
    if evidence:
        text += ": " + evidence[:EVIDENCE_WIDTH]
    return text


def render(reason, now, failures, first_seen, parts, label, counts,
           incident, digest_hours, max_list):
    """(subject, body) of a mail. parts is the dict of name lists the decision
    found: new, still, gone, held_gone and new_causes."""
    total = len(failures)
    head = SUBJECT_TAG + (" digest" if reason == "digest" else "")
    if not total:
        subject = "%s: all clear" % SUBJECT_TAG
    elif incident:
        tail = ("shared cause: %s" % label) if label else "no single cause"
        if parts["new_causes"]:
            tail += ", new cause: " + ", ".join(parts["new_causes"])
        subject = "%s: %d failing, %s" % (head, total, tail)
    elif total == 1:
        subject = "%s: 1 failing (%s)" % (head, sorted(failures)[0])
    else:
        subject = "%s: %d failing" % (head, total)
    lines = [REASON_TEXT[reason], "time: %s" % stamp(now)]
    if total:
        earliest = min(first_seen[n] for n in failures)
        lines.append("open: %d, the earliest since %s" % (total, earliest))
        lines.append("causes: " + ", ".join(
            "%s %d" % (c or "unclassified", counts[c])
            for c in sorted(counts, key=lambda c: (c == "", c))))
    for title, key in (("NEW", "new"), ("STILL OPEN", "still")):
        _section(lines, title,
                 [_describe(n, failures, first_seen) for n in parts[key]],
                 max_list)
    _section(lines, "RECOVERED", parts["gone"], max_list)
    _section(lines, "FAILED AND RECOVERED BEFORE THIS MAIL",
             parts["held_gone"], max_list)
    if incident and digest_hours > 0:
        lines.append("More failures of a cause named above wait for a digest "
                     "at most every %g hours. A new cause or a recovery is "
                     "mailed at once." % digest_hours)
    return subject, "\n".join(lines)


def decide(state, failures, now, storm_min=DEFAULT_STORM_MIN,
           digest_hours=DEFAULT_DIGEST_HOURS,
           realert_hours=DEFAULT_REALERT_HOURS, max_list=DEFAULT_MAX_LIST):
    """The pure decision. failures is {name: (cause, evidence)}, state is a
    checked state, now is an aware datetime. No I/O."""
    cur = set(failures)
    prior = set(state["signature"])
    new = sorted(cur - prior)
    gone = sorted(prior - cur)
    held_gone = sorted(set(state["pending"]) - cur)
    first_seen = {}
    for name in cur:
        seen = state["first_seen"].get(name)
        first_seen[name] = (stamp(parse_time(seen)) if parse_time(seen)
                            else stamp(now))
    label, counts = shared_cause(
        dict((n, failures[n][0]) for n in cur))
    incident = storm_min > 0 and len(cur) >= storm_min
    last_sent = parse_time(state["last_sent_utc"])

    if cur == prior and not held_gone:
        if not cur:
            return Decision(False, "ok")
        # A send time after now means the clock was set back: repeat at once
        # instead of waiting for the clock to catch up.
        due = realert_hours > 0 and (
            last_sent is None or last_sent > now
            or now - last_sent >= datetime.timedelta(hours=realert_hours))
        if not due:
            return Decision(False, "unchanged")
        reason = "realert"
    else:
        # Held only if every new failure has a cause the last mail listed.
        announced = set(state["announced"]) | {""}
        last_incident = parse_time(state["incident_utc"])
        holdable = (incident and digest_hours > 0 and bool(prior)
                    and not gone and not held_gone
                    and last_incident is not None and last_incident <= now
                    and all(failures[n][0] in announced for n in new))
        if holdable:
            due = last_incident + datetime.timedelta(hours=digest_hours)
            if now < due:
                kept = dict(state, pending=new, first_seen=first_seen)
                return Decision(False, "held", state=kept, due=due)
            reason = "digest"
        elif not prior:
            reason = "first"
        elif not cur:
            reason = "recovered"
        else:
            reason = "changed"

    # A cause the last incident mail did not list, other than the one the
    # subject already names, goes in the subject so it is not buried.
    fresh = ({failures[n][0] for n in new} - set(state["announced"])
             - {"", label}) if state["announced"] else set()
    parts = {"new": new, "still": sorted(cur & prior), "gone": gone,
             "held_gone": held_gone, "new_causes": sorted(fresh)}
    subject, body = render(reason, now, failures, first_seen, parts, label,
                           counts, incident, digest_hours, max_list)
    sent = {"version": STATE_VERSION, "signature": sorted(cur),
            "announced": sorted(c for c in counts if c) if incident else [],
            "incident_utc": stamp(now) if incident else None,
            "pending": [], "first_seen": first_seen,
            "last_sent_utc": stamp(now)}
    return Decision(True, reason, subject, body, sent)


# ------------------------------------------------------------- delivery

class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """A redirected POST is replayed as a GET with no body, and that GET can
    answer 200. Refusing the redirect makes the missed delivery an error."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def deliver_webhook(url, payload, timeout):
    """POST the payload as JSON. Raises DeliveryError, whose message never
    carries the URL: a webhook URL often holds its secret in the path."""
    data = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        url, data=data, headers={"Content-Type": "application/json"},
        method="POST")
    try:
        with urllib.request.build_opener(_NoRedirect).open(
                request, timeout=timeout):
            return
    except urllib.error.HTTPError as exc:
        raise DeliveryError("the webhook answered HTTP %d" % exc.code)
    except urllib.error.URLError as exc:
        raise DeliveryError("the webhook could not be reached (%s)"
                            % (exc.reason,))
    except Exception as exc:
        raise DeliveryError("the webhook failed (%s)" % type(exc).__name__)


def webhook_host(url):
    """host[:port] of a webhook URL, or ValueError when it is not a usable
    http or https URL. The path and query are left out of every message."""
    parts = urllib.parse.urlsplit(url)
    if parts.scheme not in ("http", "https") or not parts.hostname:
        raise ValueError("--webhook must be an http or https URL")
    if "@" in parts.netloc:
        raise ValueError("--webhook cannot carry user:password@, because "
                         "urllib does not turn it into basic auth. Put the "
                         "secret in the path or the query instead.")
    try:
        port = parts.port
    except ValueError:
        raise ValueError("--webhook has a bad port")
    return parts.hostname + (":%d" % port if port else "")


# ------------------------------------------------------------------ cli

def _finite(text):
    value = float(text)
    if not math.isfinite(value) or not 0 <= value <= MAX_NUMBER:
        raise argparse.ArgumentTypeError("%r is not a number from 0 to %g"
                                         % (text, MAX_NUMBER))
    return value


class _Parser(argparse.ArgumentParser):
    """argparse echoes the offending text of a refused flag on stderr, and
    cron mails stderr. A mistyped --web or a bad --timeout would then mail
    the webhook URL, which holds a secret. This parser names the flags and
    never the values."""

    def error(self, message):
        head, _, rest = message.partition(": ")
        if head == "unrecognized arguments":
            flags = sorted(set(t.split("=", 1)[0] for t in rest.split()
                               if t.startswith("-")))
            detail = "unrecognized flags: %s" % (", ".join(flags) or "none")
        else:
            detail = "%s: the value is missing or refused" % head
        self.print_usage(sys.stderr)
        self.exit(2, "%s: error: %s (the text typed is not echoed, because "
                  "it may hold a secret)\n" % (self.prog, detail))


def _parse(argv):
    ap = _Parser(
        prog="failstorm.py",
        description="Hold the mails about a failure storm, so the one that "
                    "matters is not lost.",
        epilog="Nothing is sent or written without --apply.",
        allow_abbrev=False,
    )
    ap.add_argument("--state", help="the state file, read every run and "
                    "written after a send or a hold")
    ap.add_argument("--failing", help="the failing file, or - for stdin")
    ap.add_argument("--cause", action="append", default=[],
                    metavar="CAUSE=REGEX",
                    help="classify evidence text. May repeat. First match "
                         "wins. No match is the blank cause.")
    ap.add_argument("--storm-min", dest="storm_min", type=int,
                    default=DEFAULT_STORM_MIN,
                    help="failures open at which a mail starts an incident, "
                         "0 for never (default %d)" % DEFAULT_STORM_MIN)
    ap.add_argument("--digest-hours", dest="digest_hours", type=_finite,
                    default=DEFAULT_DIGEST_HOURS,
                    help="hours a held failure waits after the incident mail, "
                         "0 to hold nothing (default %g)"
                         % DEFAULT_DIGEST_HOURS)
    ap.add_argument("--realert-hours", dest="realert_hours", type=_finite,
                    default=DEFAULT_REALERT_HOURS,
                    help="hours after which an unchanged set is mailed "
                         "again, 0 for never (default %g)"
                         % DEFAULT_REALERT_HOURS)
    ap.add_argument("--max-list", dest="max_list", type=int,
                    default=DEFAULT_MAX_LIST,
                    help="names listed per section of a mail (default %d)"
                         % DEFAULT_MAX_LIST)
    ap.add_argument("--webhook", help="POST the notice as JSON to this http "
                    "or https URL instead of printing it. The URL then shows "
                    "in the process list: prefer --webhook-env.")
    ap.add_argument("--webhook-env", dest="webhook_env", metavar="NAME",
                    help="read the webhook URL from this environment "
                         "variable, so a secret in it stays off the command "
                         "line")
    ap.add_argument("--timeout", type=_finite, default=DEFAULT_TIMEOUT,
                    help="seconds to wait for the webhook (default %g)"
                         % DEFAULT_TIMEOUT)
    ap.add_argument("--now", help="the time of this run as ISO 8601, for a "
                    "replay. Default is the clock.")
    ap.add_argument("--apply", action="store_true",
                    help="send the notice and write the state. Without this "
                         "nothing is sent or written.")
    ap.add_argument("--self-test", dest="self_test", action="store_true",
                    help="run the offline assertions and exit")
    return ap.parse_args(argv)


def _tolerate_odd_characters():
    """Make print() unable to fail on a character the console cannot encode.
    A failure name from a file that is not UTF-8 arrives as U+FFFD, which a
    cp1252 pipe cannot write, and the mail would never print."""
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure:
            reconfigure(errors="replace")


def _read_failing(path):
    if path == "-":
        raw = sys.stdin.buffer.read()
    else:
        with open(path, "rb") as handle:
            raw = handle.read()
    if raw.startswith((b"\xff\xfe", b"\xfe\xff")) or b"\0" in raw:
        raise ValueError("the failing file looks like UTF-16 or binary data. "
                         "Save it as UTF-8 (Windows PowerShell 5.1 '>' writes "
                         "UTF-16).")
    return raw.decode("utf-8-sig", "replace")


def main(argv=None):
    """Run the CLI. A crash exits 70, because 1 is no code this tool uses and
    a cron wrapper should not read a fault in the tool as a finding."""
    _tolerate_odd_characters()
    try:
        return _run(argv)
    except Exception as exc:
        print("error: unexpected %s: %s. This is a fault in the tool, not a "
              "finding about the jobs." % (type(exc).__name__, exc),
              file=sys.stderr)
        traceback.print_exc()
        return 70


def _usage(message):
    print("error: %s" % message, file=sys.stderr)
    return 64


def _run(argv):
    try:
        args = _parse(sys.argv[1:] if argv is None else argv)
    except SystemExit as exc:
        # argparse exits 2 on a flag it refuses, and 2 here means a file
        # could not be read. A mistyped --ap is a usage error, so 64.
        return 64 if exc.code == 2 else exc.code
    if args.self_test:
        return self_test()
    if not args.state or not args.failing:
        return _usage("--state and --failing are required. Use --self-test "
                      "to verify the tool without them.")
    if args.storm_min < 0 or args.max_list < 1 or args.timeout <= 0:
        return _usage("--storm-min cannot be negative, --max-list must be "
                      "at least 1 and --timeout must be above 0.")
    now = datetime.datetime.now(datetime.timezone.utc)
    if args.now:
        now = parse_time(args.now)
        if now is None:
            return _usage("--now is not an ISO 8601 time.")
    host = None
    webhook = args.webhook
    if args.webhook_env:
        if webhook:
            return _usage("give --webhook or --webhook-env, not both.")
        webhook = os.environ.get(args.webhook_env)
        if not webhook:
            return _usage("the environment variable %s is not set."
                          % args.webhook_env)
    try:
        rules = parse_rules(args.cause)
        if webhook:
            host = webhook_host(webhook)
    except ValueError as exc:
        return _usage(str(exc))

    try:
        raw = parse_failures(_read_failing(args.failing))
    except EmptyInput as exc:
        print("error: %s" % exc, file=sys.stderr)
        return 4
    except ValueError as exc:
        return _usage(str(exc))
    except OSError as exc:
        print("error: %s" % exc, file=sys.stderr)
        return 2
    failures = dict((n, (classify(e, rules), e)) for n, e in raw.items())

    try:
        state, warning = load_state(args.state)
    except OSError as exc:
        print("error: %s" % exc, file=sys.stderr)
        return 2
    if warning:
        print("warning: %s" % warning, file=sys.stderr)

    choice = decide(state, failures, now, args.storm_min, args.digest_hours,
                    args.realert_hours, args.max_list)

    if not args.apply:
        return _show(choice, host)
    if choice.send:
        notice = {"subject": choice.subject, "body": choice.body,
                  "reason": choice.reason, "open": len(failures)}
        if webhook:
            try:
                deliver_webhook(webhook, notice, args.timeout)
            except DeliveryError as exc:
                print("error: %s. The state was not advanced, so the next "
                      "run sends again." % exc, file=sys.stderr)
                return 3
        else:
            print("Subject: %s\n\n%s" % (choice.subject, choice.body))
    if choice.state is not None:
        try:
            save_state(args.state, choice.state)
        except OSError as exc:
            print("error: could not write the state: %s. %s" % (
                exc, "The notice WAS delivered, so the next run sends it "
                "again." if choice.send else "Nothing was held."),
                file=sys.stderr)
            return 2
    return 0


def _show(choice, host):
    """The dry-run report. It is the only output of a run without --apply."""
    if choice.send:
        route = "webhook on %s" % host if host else "stdout, which cron mails"
        print("failstorm check only: SEND (%s) by %s" % (choice.reason, route))
        print("Subject: %s\n\n%s" % (choice.subject, choice.body))
    elif choice.reason == "held":
        print("failstorm check only: HOLD until %s" % stamp(choice.due))
    else:
        print("failstorm check only: NOTHING (%s)" % choice.reason)
    print("\nNothing was sent and nothing was written. Re-run with --apply.")
    return 0


# ------------------------------------------------------------- self-test

def self_test():
    """Run the assertions in a temp directory that is removed even when one
    of them raises."""
    tmp = tempfile.mkdtemp(prefix="failstorm-selftest-")
    try:
        return _self_test(tmp)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


class _Stub(object):
    """A local HTTP server that records POST bodies and answers a chosen
    status. mode 'slow' holds the answer until released."""

    def __init__(self, status=200):
        self.status = status
        self.seen = []
        self.release = threading.Event()
        stub = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                length = int(self.headers.get("Content-Length") or 0)
                stub.seen.append(("POST", self.path, self.rfile.read(length)))
                if stub.status == "slow":
                    stub.release.wait(3)
                    return
                self.send_response(stub.status)
                if stub.status == 302:
                    self.send_header("Location", "/elsewhere")
                self.send_header("Content-Length", "0")
                self.end_headers()

            def do_GET(self):
                stub.seen.append(("GET", self.path, b""))
                self.send_response(200)
                self.send_header("Content-Length", "0")
                self.end_headers()

            def log_message(self, *args):
                pass

        self.server = HTTPServer(("127.0.0.1", 0), Handler)
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever)
        self.thread.daemon = True
        self.thread.start()

    def url(self, path="/hook"):
        return "http://127.0.0.1:%d%s" % (self.port, path)

    def close(self):
        self.release.set()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(5)


def _self_test(tmp):
    """Assertions over the decision core and the CLI. No credentials. The only
    network is a stub on 127.0.0.1, and the only files are under tmp."""
    passed = [0]
    failed = []

    def check(cond, label):
        if cond:
            passed[0] += 1
            print("PASS  %s" % label)
        else:
            failed.append(label)
            print("FAIL  %s" % label)

    def raises(fn, label, kind=ValueError):
        try:
            fn()
        except kind:
            check(True, label)
        except Exception as exc:
            check(False, "%s (wrong exception %r)" % (label, exc))
        else:
            check(False, "%s (no error raised)" % label)

    print("failstorm self-test: temporary files only, a stub on 127.0.0.1, "
          "no credentials")
    print("-" * 68)

    base = datetime.datetime(2026, 3, 3, 0, 0, tzinfo=datetime.timezone.utc)

    def at(hours):
        return base + datetime.timedelta(hours=hours)

    def fails(**named):
        """{name: (cause, evidence)} from name=cause keywords."""
        return dict((n, (c, "")) for n, c in named.items())

    def step(state, failures, hours, **options):
        choice = decide(state, failures, at(hours), **options)
        return choice, (choice.state if choice.state is not None else state)

    def run(argv, stdin=None):
        out, err = io.StringIO(), io.StringIO()
        saved = sys.stdin
        if stdin is not None:
            sys.stdin = type("In", (), {"buffer": io.BytesIO(stdin)})()
        try:
            with contextlib.redirect_stdout(out):
                with contextlib.redirect_stderr(err):
                    code = main(argv)
        finally:
            sys.stdin = saved
        return code, out.getvalue(), err.getvalue()

    def write(name, text):
        path = os.path.join(tmp, name)
        with open(path, "wb") as handle:
            handle.write(text if isinstance(text, bytes)
                         else text.encode("utf-8"))
        return path

    def read(path):
        with open(path, "rb") as handle:
            return handle.read().decode("utf-8")

    # ---- time
    check(parse_time("2026-03-03T01:00:00Z") == at(1),
          "a Z suffix is UTC")
    check(parse_time("2026-03-03T00:00:00") == at(0),
          "a time with no offset is read as UTC")
    check(parse_time("2026-03-02T19:00:00-05:00") == at(0),
          "an offset is converted to UTC")
    check(parse_time("last tuesday") is None and parse_time(None) is None
          and parse_time("") is None, "text that is not a time is None")
    check(stamp(at(5)) == "2026-03-03T05:00:00Z", "stamp writes UTC with a Z")
    for text in ("0001-01-01T00:00:00+05:00", "0001-01-01T00:00:00Z",
                 "9999-12-31T23:59:59Z", "1800-01-01T00:00:00Z"):
        check(parse_time(text) is None,
              "the time %s is out of range and is not a time, where it "
              "used to crash with OverflowError  <-- pinned defect" % text)

    # ---- the failing file
    got = parse_failures("a\tno space left\nb\n\n# a note\nc\t\r\n")
    check(got == {"a": "no space left", "b": "", "c": ""},
          "a name, an optional tab and evidence per line; blanks and "
          "comments skipped; CRLF stripped")
    check(parse_failures("#ok\n") == {}, "#ok alone means a clean run")
    check(parse_failures("#OK  \n") == {}, "#ok is case and space tolerant")
    raises(lambda: parse_failures(""),
           "an empty file is refused, a crashed producer writes one"
           "  <-- pinned defect", EmptyInput)
    raises(lambda: parse_failures("\n  \n# only a note\n"),
           "blank lines and comments are still an empty file", EmptyInput)
    raises(lambda: parse_failures("a\na\n"),
           "a repeated name is refused, not merged")
    raises(lambda: parse_failures("\tevidence\n"),
           "evidence with no name is refused")
    raises(lambda: parse_failures("a\n#ok\n"),
           "#ok beside a failure is refused")
    check(parse_failures("  #ok\n") == {} and parse_failures("\t#ok\n") == {},
          "#ok after blanks is still an all clear, not a failure named "
          "#ok  <-- pinned defect")
    check(parse_failures("a\n   # note\n") == {"a": ""},
          "a line whose first visible character is # is a comment")

    # ---- rules and causes
    rules = parse_rules(["disk=no space|i/o error", "auth=token"])
    check(classify("ERROR No space left", rules) == "disk",
          "a rule matches without regard to case")
    check(classify("token expired", rules) == "auth", "a second rule matches")
    check(classify("no space, token", rules) == "disk",
          "the first matching rule wins")
    check(classify("all fine", rules) == "", "no match is the blank cause")
    for bad in ("nocause", "=x", "disk=", "disk=("):
        raises(lambda b=bad: parse_rules([b]),
               "the rule %r is refused" % bad)
    check(shared_cause({"a": "disk", "b": "disk", "c": "auth"})[0] == "disk",
          "two of three is a strict majority")
    check(shared_cause({"a": "disk", "b": "auth"})[0] == "",
          "a tie has no shared cause")
    check(shared_cause({"a": "", "b": "", "c": "auth"})[0] == "",
          "the blank cause never wins, however many there are")
    check(shared_cause({"a": "disk", "b": "", "c": ""})[0] == "",
          "one named cause among blanks is not a majority")
    check(shared_cause({}) == ("", {}), "no failures have no cause")

    # ---- the decision, one tick at a time
    fresh = new_state()
    choice, _ = step(fresh, {}, 0)
    check(not choice.send and choice.reason == "ok" and choice.state is None,
          "nothing failing and nothing announced is quiet and writes nothing")
    choice, state = step(fresh, fails(a="disk"), 0)
    check(choice.send and choice.reason == "first"
          and choice.subject == "failstorm: 1 failing (a)",
          "a first failure is mailed at once, named in the subject")
    check(state["incident_utc"] is None and state["announced"] == [],
          "one failure is not an incident")
    choice, state = step(state, fails(a="disk"), 1)
    check(not choice.send and choice.reason == "unchanged"
          and choice.state is None, "the same failure an hour later is quiet")
    choice, _ = step(state, fails(a="disk"), 25)
    check(choice.send and choice.reason == "realert"
          and "STILL OPEN (1)" in choice.body,
          "the same failure a day later is mailed again as still open")
    choice, _ = step(state, fails(a="disk"), 25, realert_hours=0)
    check(not choice.send, "--realert-hours 0 never repeats")
    gap = dict(state, last_sent_utc=None)
    choice, _ = step(gap, fails(a="disk"), 1)
    check(choice.send and choice.reason == "realert",
          "a state with no send time repeats rather than stay silent")
    choice, _ = step(state, fails(a="disk"), 24)
    check(choice.send and choice.reason == "realert",
          "the repeat is due at exactly --realert-hours")
    choice, _ = step(state, fails(a="disk"), 23.99)
    check(not choice.send and choice.reason == "unchanged",
          "and not a minute before")
    ahead = dict(state, last_sent_utc=stamp(at(9)))
    choice, _ = step(ahead, fails(a="disk"), 1)
    check(choice.send and choice.reason == "realert",
          "a send time in the future (clock set back) repeats at once "
          "instead of muting the tool  <-- pinned defect")
    check(check_state(new_state()) == new_state(),
          "a state with null incident and send times is well formed")

    three = fails(a="disk", b="disk", c="disk")
    choice, storm = step(fresh, three, 0)
    check(choice.send and choice.reason == "first"
          and choice.subject == "failstorm: 3 failing, shared cause: disk",
          "three failures with one cause start an incident naming it")
    check(storm["announced"] == ["disk"] and storm["incident_utc"]
          == stamp(at(0)), "the incident mail records what it announced")
    check("causes: disk 3" in choice.body, "the mail lists the causes it saw")

    four = dict(three, d=("disk", ""))
    choice, held = step(storm, four, 1)
    check(not choice.send and choice.reason == "held"
          and choice.due == at(6), "more of the same cause is held")
    check(held["pending"] == ["d"] and held["signature"] == storm["signature"]
          and held["last_sent_utc"] == storm["last_sent_utc"],
          "a hold records what waits and moves nothing else")
    five = dict(four, e=("disk", ""))
    choice, held = step(held, five, 3)
    check(choice.reason == "held" and held["pending"] == ["d", "e"],
          "a second held failure joins the first")
    choice, _ = step(held, five, 5.99)
    check(not choice.send, "still held a minute before the digest is due")
    choice, after = step(held, five, 6)
    check(choice.send and choice.reason == "digest"
          and choice.subject.startswith("failstorm digest: 5 failing")
          and "NEW (2):" in choice.body and "STILL OPEN (3):" in choice.body,
          "the digest is sent when due and lists what was held")
    check(after["pending"] == [] and after["incident_utc"] == stamp(at(6)),
          "a digest clears the hold and starts the next wait")
    check(held["first_seen"]["d"] == stamp(at(1))
          and after["first_seen"]["d"] == stamp(at(1)),
          "a held failure keeps the time it was first seen")

    offset = dict(storm, first_seen={"a": "2026-03-02T19:00:00-05:00",
                                     "b": "garbage"})
    choice, _ = step(offset, three, 1, realert_hours=0)
    check("the earliest since 2026-03-03T00:00:00Z" in
          step(offset, dict(three, z=("auth", "")), 1)[0].body
          and choice.reason == "unchanged",
          "a first-seen time with an offset is written back as UTC, and one "
          "that is not a time is replaced by now")

    # ---- the majority flip
    def naive_holds(announced_label, failures_now, new_names):
        """The wrong rule: compare each new failure's cause with the
        majority cause of everything open now."""
        label = shared_cause(dict((n, c[0]) for n, c in
                                  failures_now.items()))[0]
        return all(failures_now[n][0] in (label, "") for n in new_names)

    flip = dict(three, **fails(w="auth", x="auth", y="auth", z="auth"))
    choice, _ = step(storm, flip, 1)
    check(choice.send and choice.reason == "changed"
          and choice.subject.endswith("shared cause: auth"),
          "four new failures of another cause outnumber three, and are "
          "sent at once  <-- pinned defect")
    check(naive_holds("disk", flip, ["w", "x", "y", "z"]),
          "the rule that compares with the majority open now holds them, so "
          "this case can tell the two rules apart")
    minority = dict(three, **fails(w="auth", x="auth"))
    choice, _ = step(storm, minority, 1)
    check(choice.send and choice.reason == "changed"
          and choice.subject == "failstorm: 5 failing, shared cause: disk, "
          "new cause: auth",
          "two new failures of another cause are sent at once, and the "
          "subject names the cause that is new")
    check(not naive_holds("disk", minority, ["w", "x"]),
          "and the majority rule also sends them, which is why the flip "
          "needs four")
    mixed = dict(four, **fails(w="auth"))
    choice, _ = step(storm, mixed, 1)
    check(choice.send, "one new failure of another cause breaks the hold "
          "even beside a held one")
    unknown = dict(three, q=("", "who knows"))
    choice, _ = step(storm, unknown, 1)
    check(not choice.send and choice.reason == "held",
          "a failure with no recognised cause waits, like more of the same")
    choice, _ = step(storm, unknown, 1, digest_hours=0)
    check(choice.send and choice.reason == "changed",
          "--digest-hours 0 holds nothing, and what it sends is a change, "
          "not an empty digest  <-- pinned defect")
    choice, _ = step(storm, four, 1, storm_min=0)
    check(choice.send and "shared cause" not in choice.subject,
          "--storm-min 0 never starts an incident, so nothing is held")
    choice, _ = step(storm, four, 0)
    check(not choice.send and choice.reason == "held",
          "a run at exactly the incident time still holds  <-- pinned "
          "defect")

    # ---- the layout of a mail
    choice, _ = step(fresh, three, 0)
    check("(0)" not in choice.body and "STILL OPEN" not in choice.body
          and "RECOVERED" not in choice.body,
          "a first mail has no empty section  <-- pinned defect")
    check("wait for a digest at most every 6 hours" in choice.body,
          "an incident mail tells the reader what is held")
    choice, _ = step(fresh, three, 0, digest_hours=0)
    check("wait for a digest" not in choice.body,
          "with --digest-hours 0 the mail does not promise a digest  <-- "
          "pinned defect")
    choice, _ = step(fresh, fails(a="disk"), 0)
    check("wait for a digest" not in choice.body,
          "a single failure is no incident, so the mail does not promise a "
          "digest  <-- pinned defect")
    choice, _ = step(fresh, fails(a="disk", b="disk", c="auth"), 0)
    check(choice.subject == "failstorm: 3 failing, shared cause: disk",
          "the first incident mail names no new cause, whatever its mix  "
          "<-- pinned defect")
    ordered = dict(("n%02d" % i, ("disk", "")) for i in range(12))
    choice, one = step(fresh, ordered, 0)
    choice, _ = step(one, dict(ordered, z=("auth", "")), 1)
    listed = choice.body.split("STILL OPEN (12):\n")[1].split("\nMore")[0]
    check([line.split()[0] for line in listed.split("\n")]
          == sorted(ordered), "STILL OPEN lists names in sorted order, not "
          "hash order  <-- pinned defect")

    # ---- what always goes at once
    choice, rec = step(storm, fails(a="disk", b="disk"), 1)
    check(choice.send and choice.reason == "changed"
          and "RECOVERED (1):" in choice.body,
          "a recovery inside an incident is sent at once")
    choice, rec = step(storm, {}, 1)
    check(choice.send and choice.reason == "recovered"
          and choice.subject == "failstorm: all clear"
          and rec["signature"] == [] and rec["incident_utc"] is None,
          "the all clear is sent and closes the incident")
    choice, _ = step(rec, three, 2)
    check(choice.send and choice.reason == "first",
          "a failure after an all clear starts a new incident, not a hold")
    swap = dict(fails(a="disk", b="disk", d="disk"))
    choice, _ = step(storm, swap, 1)
    check(choice.send and choice.reason == "changed",
          "one recovered and one new at the same time is sent, not held")
    forged = dict(storm, signature=[])
    choice, _ = step(forged, three, 1)
    check(choice.send and choice.reason == "first",
          "a state with an incident time but no open failures does not hold")
    future = dict(storm, incident_utc=stamp(at(9)))
    choice, _ = step(future, four, 1)
    check(choice.send, "an incident time in the future never holds, a clock "
          "set back must not mute the tool")
    nostamp = dict(storm, incident_utc=None)
    choice, _ = step(nostamp, four, 1)
    check(choice.send, "no recorded incident mail means nothing to hold "
          "behind")

    # ---- a held failure that recovers before its digest
    choice, held = step(storm, four, 1)
    choice, back = step(held, three, 2)
    check(choice.send and choice.reason == "changed"
          and "FAILED AND RECOVERED BEFORE THIS MAIL (1):" in choice.body
          and "\n  d\n" in choice.body + "\n",
          "a held failure that recovers is still reported once  "
          "<-- pinned defect")
    check(back["pending"] == [] and back["signature"] == ["a", "b", "c"],
          "and the hold is cleared by that mail")
    choice, held = step(storm, four, 1)
    choice, _ = step(held, {}, 2)
    check(choice.send and choice.reason == "recovered",
          "a held failure and the rest recovering together is one all clear")

    # ---- list bounds
    many = dict(("n%02d" % i, ("disk", "")) for i in range(30))
    choice, _ = step(fresh, many, 0, max_list=5)
    check("NEW (30):" in choice.body and "... and 25 more" in choice.body
          and "n04" in choice.body and "n05" not in choice.body,
          "a long list is cut at --max-list and says how many it left out")
    choice, _ = step(fresh, many, 0, max_list=30)
    check("and" not in choice.body.split("NEW (30):")[1].split("More")[0]
          .replace("\n", " ").split("n29")[1],
          "a list that fits is printed whole")
    ev = {"a": ("disk", "x" * 200)}
    choice, _ = step(fresh, ev, 0)
    check(("x" * 80) in choice.body and ("x" * 81) not in choice.body,
          "evidence is cut to 80 characters")
    ties = fails(a="disk", b="auth")
    choice, _ = step(fresh, ties, 0, storm_min=2)
    check("no single cause" in choice.subject and
          choice.body.index("causes: auth 1, disk 1") > 0,
          "an incident with no majority says so and lists every cause")
    blank = {"a": ("", ""), "b": ("disk", ""), "c": ("", "")}
    choice, _ = step(fresh, blank, 0)
    check("causes: disk 1, unclassified 2" in choice.body,
          "unclassified is listed last")

    # ---- a replay of a synthetic storm
    timeline = {
        1: fails(j1="disk", j2="disk", j3="disk"),
        2: fails(j1="disk", j2="disk", j3="disk", j4="disk"),
        3: fails(j1="disk", j2="disk", j3="disk", j4="disk", j5="disk"),
        4: fails(j1="disk", j2="disk", j3="disk", j4="disk", j5="disk",
                 j6="disk"),
        5: fails(j1="disk", j2="disk", j3="disk", j4="disk", j5="disk",
                 j6="disk", j7="auth"),
        6: fails(j1="disk", j2="disk", j3="disk", j4="disk", j5="disk",
                 j6="disk", j7="auth", j8="disk"),
        7: fails(j2="disk", j3="disk", j4="disk", j5="disk", j6="disk",
                 j7="auth", j8="disk"),
        8: fails(j3="disk", j4="disk", j5="disk", j6="disk", j7="auth",
                 j8="disk", j9="disk"),
        9: fails(j4="disk", j5="disk", j6="disk", j7="auth", j8="disk",
                 j9="disk"),
        10: fails(j5="disk", j6="disk", j7="auth", j8="disk"),
        11: fails(j7="auth"),
    }

    def replay(**options):
        state, mails = new_state(), []
        for hour in range(0, 14):
            choice, state = step(state, timeline.get(hour, {}), hour,
                                 **options)
            if choice.send:
                mails.append((hour, choice.reason))
        return mails

    plain = replay(storm_min=0, realert_hours=0)
    held_run = replay(realert_hours=0)
    check(len(plain) == 12 and len(held_run) == 8,
          "the same 14 hourly ticks send 12 mails when every change is "
          "mailed and 8 with the hold")
    check(held_run == [(1, "first"), (5, "changed"), (7, "changed"),
                       (8, "changed"), (9, "changed"), (10, "changed"),
                       (11, "changed"), (12, "recovered")],
          "and the 8 are the first mail, the new cause, the recoveries and "
          "the all clear; hours 2, 3, 4 and 6 were held")
    check(all(r != "digest" for _, r in held_run),
          "no digest was due in a replay that ends before the wait")

    # ---- the state file
    path = os.path.join(tmp, "state.json")
    check(load_state(path) == (new_state(), None),
          "a missing state file is a fresh state with no warning")
    save_state(path, storm)
    check(load_state(path) == (storm, None), "a saved state loads back equal")
    check(not [f for f in os.listdir(tmp) if f.startswith(".failstorm-")],
          "saving leaves no temp file behind")
    for label, text in (("a state of text that is not JSON", "{not json"),
                        ("JSON of the wrong shape", "[1, 2]"),
                        ("a deeply nested array", "[" * 100000),
                        ("a state of another version",
                         '{"version": 9}')):
        write("bad.json", text)
        state, warning = load_state(os.path.join(tmp, "bad.json"))
        check(state == new_state() and warning and "unusable" in warning,
              "%s is treated as empty with a warning, never as silence"
              "  <-- pinned defect" % label)
    base_state = new_state()
    for label, key, value in (
            ("a signature that is not a list", "signature", "a"),
            ("a signature of numbers", "signature", [1]),
            ("pending that is not a list", "pending", {}),
            ("first_seen that is not a map", "first_seen", []),
            ("first_seen of numbers", "first_seen", {"a": 1}),
            ("an incident time that is not a time", "incident_utc", "soon"),
            ("an incident time that is a number", "last_sent_utc", 5)):
        raises(lambda k=key, v=value: check_state(dict(base_state, **{k: v})),
               "%s is refused" % label)
    for key in ("incident_utc", "last_sent_utc"):
        gapped = dict(new_state())
        del gapped[key]
        raises(lambda g=gapped: check_state(g),
               "a state with no %s is refused, where it used to crash with "
               "KeyError  <-- pinned defect" % key)
    raises(lambda: check_state(dict(
        base_state, incident_utc="0001-01-01T00:00:00+14:00")),
        "an incident time that overflows is refused, where it used to "
        "crash with OverflowError  <-- pinned defect")
    write("bom.json", b"\xef\xbb\xbf" + json.dumps(storm).encode("utf-8"))
    check(load_state(os.path.join(tmp, "bom.json"))[0] == storm,
          "a state saved with a byte order mark still loads")
    raises(lambda: save_state(os.path.join(tmp, "nodir", "s.json"), storm),
           "saving into a missing folder fails loudly", OSError)
    trap = os.path.join(tmp, "trap")
    os.makedirs(os.path.join(trap, "s.json"))
    raises(lambda: save_state(os.path.join(trap, "s.json"), storm),
           "saving over a folder fails loudly", OSError)
    check(os.listdir(trap) == ["s.json"],
          "and a failed save removes its temp file, which sits beside the "
          "state path  <-- pinned defect")
    raises(lambda: load_state(tmp),
           "a state path that is a folder cannot be read", OSError)

    # ---- the command line, dry run first
    live = write("live.txt", "a\tno space left\nb\tno space left\n"
                 "c\tI/O error\n")
    st = os.path.join(tmp, "cli-state.json")
    code, out, err = run(["--state", st, "--failing", live,
                          "--cause", "disk=no space|i/o"])
    check(code == 0 and "SEND (first) by stdout, which cron mails" in out
          and "shared cause: disk" in out and not os.path.exists(st),
          "a run without --apply shows the mail and writes nothing")
    check("Nothing was sent and nothing was written" in out and err == "",
          "and says so")
    code, out, err = run(["--state", st, "--failing", live, "--apply",
                          "--cause", "disk=no space|i/o"])
    check(code == 0 and out.startswith("Subject: failstorm: 3 failing, "
                                       "shared cause: disk\n\n")
          and os.path.exists(st), "--apply prints the mail and saves state")
    code, out, err = run(["--state", st, "--failing", live, "--apply",
                          "--cause", "disk=no space|i/o"])
    check(code == 0 and out == "" and err == "",
          "the next run with nothing new prints nothing at all")
    code, out, _ = run(["--state", st, "--failing", live,
                        "--cause", "disk=no space|i/o"])
    check("NOTHING (unchanged)" in out, "a dry run names an unchanged set")
    grown = write("grown.txt", read(live) + "d\tno space left\n")
    code, out, _ = run(["--state", st, "--failing", grown,
                        "--cause", "disk=no space|i/o"])
    check("HOLD until 2" in out, "a dry run names the time a hold ends")
    before = read(st)
    code, out, err = run(["--state", st, "--failing", grown, "--apply",
                          "--cause", "disk=no space|i/o"])
    check(code == 0 and out == "" and err == ""
          and json.loads(read(st))["pending"] == ["d"]
          and json.loads(read(st))["signature"]
          == json.loads(before)["signature"],
          "a held run prints nothing, so cron mails nothing, yet saves the "
          "hold  <-- pinned defect")

    lone = write("lone.txt", "solo\tdisk error\n")
    lone_state = os.path.join(tmp, "lone.json")
    code, out, err = run(["--state", lone_state, "--failing", lone, "--apply"])
    check(code == 0 and out.startswith("Subject: failstorm: 1 failing (solo)")
          and "(0)" not in out, "a single failure through the command line "
          "is mailed")
    code, out, err = run(["--state", lone_state, "--failing", lone, "--apply"])
    check(code == 0 and out == "" and err == "",
          "and the next run prints nothing and no warning, so its state "
          "with null times was read as good  <-- pinned defect")

    # ---- stdin, byte order marks, bad bytes
    code, out, _ = run(["--state", os.path.join(tmp, "x.json"),
                        "--failing", "-"],
                       stdin=b"\xef\xbb\xbfalpha\r\nbeta\r\n")
    check(code == 0 and "alpha [unclassified]" in out
          and "\ufeff" not in out, "stdin works and a byte order mark is "
          "not part of the first name")
    code, out, _ = run(["--state", os.path.join(tmp, "x.json"),
                        "--failing", "-"], stdin=b"caf\xe9\n")
    check(code == 0 and "caf" in out, "bytes that are not UTF-8 are replaced, "
          "not a crash")
    u16 = os.path.join(tmp, "u16-state.json")
    for label, blob in (("a UTF-16 file with a byte order mark",
                         "#ok\n".encode("utf-16")),
                        ("a UTF-16 file without one",
                         "#ok\n".encode("utf-16-le")),
                        ("a file with a NUL byte", b"a\x00b\n")):
        code, out, err = run(["--state", u16, "--failing",
                              write("u16.txt", blob), "--apply"])
        check(code == 64 and "UTF-16" in err and out == ""
              and not os.path.exists(u16),
              "%s is refused with exit 64, not read as garbled names  <-- "
              "pinned defect" % label)

    # ---- refusals and exit codes
    ok_file = write("ok.txt", "#ok\n")
    empty_file = write("empty.txt", "")
    code, _, err = run(["--state", st, "--failing", empty_file, "--apply"])
    check(code == 4 and "all clear" in err
          and json.loads(read(st))["pending"] == ["d"],
          "an empty failing file exits 4 and leaves the state alone"
          "  <-- pinned defect")
    code, out, _ = run(["--state", st, "--failing", ok_file, "--apply"])
    check(code == 0 and "all clear" in out and
          json.loads(read(st))["signature"] == [],
          "#ok is a real all clear")
    code, out, err = run(["--state", st, "--failing",
                          os.path.join(tmp, "missing.txt")])
    check(code == 2 and err.startswith("error: ") and "missing.txt" in err
          and out == "", "a missing failing file exits 2 and says which")
    code, out, err = run(["--state", tmp, "--failing", live])
    check(code == 2 and err.startswith("error: ") and out == "",
          "a state path that cannot be read exits 2 and says why")
    dup = write("dup.txt", "a\na\n")
    code, _, err = run(["--state", st, "--failing", dup])
    check(code == 64 and "line 2 repeats the name 'a'" in err,
          "a repeated name exits 64 and the message names the line")
    check(run([])[0] == 64, "no arguments exit 64")
    check(run(["--state", st])[0] == 64, "no --failing exits 64")
    check(run(["--state", st, "--failing", live, "--storm-min", "-1"])[0] == 64
          and run(["--state", st, "--failing", live, "--max-list", "0"])[0]
          == 64, "a negative --storm-min and a zero --max-list exit 64")
    for flag in ("--digest-hours", "--realert-hours", "--timeout"):
        for value in ("nan", "inf", "-1", "soon"):
            code, _, err = run(["--state", st, "--failing", live, flag,
                                value])
            check(code == 64, "%s %s exits 64" % (flag, value))
    for flag in ("--digest-hours", "--realert-hours", "--timeout"):
        code, _, err = run(["--state", st, "--failing", live, flag, "1e300"])
        check(code == 64, "%s 1e300 exits 64, where it used to overflow "
              "datetime and exit 70  <-- pinned defect" % flag)
    check(run(["--state", st, "--failing", live, "--timeout", "0"])[0] == 64,
          "--timeout 0 exits 64, because it makes every delivery fail")
    for value in ("0001-01-01T00:00:00+05:00", "yesterday"):
        check(run(["--state", st, "--failing", live, "--now", value])[0]
              == 64, "--now %s exits 64  <-- pinned defect" % value)
    # Valid values reach decide() through the real command line.
    code, out, err = run(["--state", st, "--failing", live,
                          "--digest-hours", "0", "--realert-hours", "0",
                          "--storm-min", "0", "--max-list", "1"])
    check(code == 0 and "NEW (3):" in out and "... and 2 more" in out
          and "wait for a digest" not in out and err == "",
          "--digest-hours 0, --realert-hours 0, --storm-min 0 and "
          "--max-list 1 are accepted and take effect")
    code, out, err = run(["--state", st, "--failing", live,
                          "--digest-hours", "0.5", "--realert-hours", "12",
                          "--timeout", "3"])
    check(code == 0 and "wait for a digest at most every 0.5 hours" in out
          and err == "", "valid --digest-hours, --realert-hours and "
          "--timeout values are accepted")
    check(run(["--state", st, "--failing", live, "--cause", "oops"])[0] == 64,
          "a bad --cause exits 64")
    for url in ("file:///etc/passwd", "ftp://host/x", "http://",
                "http://host:port/x", "host/path"):
        check(run(["--state", st, "--failing", live, "--webhook", url])[0]
              == 64, "the webhook %s is refused" % url)
    code, _, err = run(["--state", st, "--failing", live, "--webhook",
                        "http://user:Hunter2@127.0.0.1:9/x", "--apply"])
    check(code == 64 and "user:password@" in err and "Hunter2" not in err,
          "a webhook with user:password@ is refused with exit 64, not "
          "retried forever as exit 3, and the password is not echoed  <-- "
          "pinned defect")
    check(webhook_host("https://h:8443/x") == "h:8443"
          and webhook_host("http://h/x") == "h",
          "an https webhook URL is accepted and reduced to host:port")
    check(run(["-h"])[0] == 0, "-h exits 0")

    # ---- a refused flag never echoes the text typed after it, because cron
    # mails stderr and a webhook URL holds a secret
    for kind, argv, named in (
            ("a mistyped flag", ["--web", "http://127.0.0.1:9/T0K3N",
                                 "--apply"], "--web"),
            ("a mistyped flag joined with =", ["--webhook=http://x/T0K3N",
                                               "--bogus=T0K3N"], "--bogus"),
            ("a number that is not one", ["--timeout", "http://x/T0K3N"],
             "--timeout"),
            ("a value given to a switch", ["--apply=T0K3N"], "--apply"),
            ("an integer that is not one", ["--storm-min", "T0K3N"],
             "--storm-min"),
            ("a flag with no value", ["--webhook"], "--webhook"),
            ("a stray word", ["T0K3N"], "unrecognized flags: none")):
        code, out, err = run(["--state", st, "--failing", live] + argv)
        check(code == 64 and "T0K3N" not in err + out and named in err
              and "usage:" in err,
              "%s is refused with exit 64, names the flag and does not echo "
              "the text  <-- pinned defect" % kind)

    # ---- abbreviations. Each case is a run that would succeed, or write, if
    # the prefix were accepted, so a refusal is not an unrelated usage error.
    guard = os.path.join(tmp, "abbrev.json")
    for short in ("--ap", "--app", "--appl"):
        code, out, err = run(["--state", guard, "--failing", live, short])
        check(code == 64 and out == "" and not os.path.exists(guard),
              "the flag prefix %s is refused with exit 64 and writes "
              "nothing  <-- pinned defect" % short)
    for short in ("--stat", "--sta", "--st"):
        code, out, err = run(["--failing", live, short, guard, "--apply"])
        check(code == 64 and out == "" and not os.path.exists(guard),
              "the option prefix %s is refused with exit 64  <-- pinned "
              "defect" % short)
    for short, value in (("--fail", live), ("--cau", "disk=x"),
                         ("--web", "http://127.0.0.1:9/x"), ("--time", "1"),
                         ("--digest", "1"), ("--no", "2026-03-03"),
                         ("--max", "3")):
        code, out, err = run(["--state", guard, "--failing", live, short,
                              value, "--apply"])
        check(code == 64 and out == "" and not os.path.exists(guard),
              "the option prefix %s is refused with exit 64  <-- pinned "
              "defect" % short)

    # ---- the webhook, against a stub
    secret = "/hooks/T0K3N-SECRET-VALUE"
    wh_state = os.path.join(tmp, "wh.json")
    stub = _Stub(200)
    saved_env = dict(os.environ)
    os.environ["no_proxy"] = os.environ["NO_PROXY"] = "127.0.0.1"
    try:
        code, out, err = run(["--state", wh_state, "--failing", live,
                              "--webhook", stub.url(secret), "--apply"])
        body = json.loads(stub.seen[0][2].decode("utf-8")) if stub.seen else {}
        check(code == 0 and out == "" and len(stub.seen) == 1
              and stub.seen[0][:2] == ("POST", secret)
              and body.get("reason") == "first" and body.get("open") == 3
              and body.get("subject", "").startswith("failstorm: 3 failing"),
              "the webhook receives one POST of the notice as JSON")
        check(json.loads(read(wh_state))["signature"] == ["a", "b", "c"],
              "and the state is saved after the delivery")
        code, out, _ = run(["--state", wh_state, "--failing", ok_file,
                            "--webhook", stub.url(secret)])
        check(code == 0 and "by webhook on 127.0.0.1:%d" % stub.port in out
              and "T0K3N" not in out and len(stub.seen) == 1,
              "a dry run names the webhook host only and posts nothing")

        stub.status = 500
        saved = read(wh_state)
        code, out, err = run(["--state", wh_state, "--failing", ok_file,
                              "--webhook", stub.url(secret), "--apply"])
        check(code == 3 and "HTTP 500" in err and "state was not advanced"
              in err and read(wh_state) == saved and out == "",
              "a webhook that answers 500 exits 3 and leaves the state, so "
              "the next run sends again  <-- pinned defect")
        stub.status = 404
        code, out, err = run(["--state", wh_state, "--failing", ok_file,
                              "--webhook", stub.url(secret), "--apply"])
        check(code == 3 and "HTTP 404" in err and read(wh_state) == saved,
              "a webhook that answers 404 exits 3 as well")
        stub.status = 302
        stub.seen[:] = []
        code, out, err = run(["--state", wh_state, "--failing", ok_file,
                              "--webhook", stub.url(secret), "--apply"])
        check(code == 3 and "HTTP 302" in err
              and [s[0] for s in stub.seen] == ["POST"],
              "a redirect is a failed delivery, and is not followed as a GET"
              "  <-- pinned defect")
        stub.seen[:] = []
        with urllib.request.urlopen(urllib.request.Request(
                stub.url(secret), data=b"{}", method="POST")):
            pass
        check([x[0] for x in stub.seen] == ["POST", "GET"],
              "control: the default opener does follow a redirect, as a GET "
              "with the body gone, which is why delivery refuses to")
        stub.status = "slow"
        started = time.monotonic()
        code, out, err = run(["--state", wh_state, "--failing", ok_file,
                              "--webhook", stub.url(secret), "--apply",
                              "--timeout", "0.3"])
        check(code == 3 and "failed (" in err and read(wh_state) == saved,
              "a webhook that does not answer in time exits 3")
        check(time.monotonic() - started < 2.0,
              "and it gives up at --timeout (0.3 s) while the stub would "
              "hold the call for 3 s")
        stub.release.set()
        # https: a stub with no certificate cannot finish a TLS handshake,
        # so this shows the https path is taken and fails closed.
        stub.status = 200
        stub.seen[:] = []
        code, out, err = run(["--state", wh_state, "--failing", ok_file,
                              "--webhook", "https://127.0.0.1:%d%s"
                              % (stub.port, secret), "--apply",
                              "--timeout", "3"])
        check(code == 3 and "webhook" in err and "T0K3N" not in err + out
              and read(wh_state) == saved and not stub.seen,
              "an https webhook is tried over TLS, fails closed with exit 3 "
              "against a plain http stub, and the state is not advanced")
        stub.release.set()
        stub.status = 200
        stub.seen[:] = []
        code, out, err = run(["--state", wh_state, "--failing", ok_file,
                              "--webhook", stub.url(secret), "--apply"])
        check(code == 0 and len(stub.seen) == 1
              and json.loads(read(wh_state))["signature"] == [],
              "after the stub recovers the same run delivers and advances")
        # The URL from the environment, so the secret stays off argv.
        os.environ["FS_TEST_HOOK"] = stub.url(secret)
        stub.seen[:] = []
        code, out, err = run(["--state", os.path.join(tmp, "wh-env.json"),
                              "--failing", live, "--webhook-env",
                              "FS_TEST_HOOK", "--apply"])
        check(code == 0 and out == "" and err == "" and len(stub.seen) == 1
              and stub.seen[0][:2] == ("POST", secret),
              "--webhook-env delivers to the URL held in that environment "
              "variable")
        code, out, err = run(["--state", wh_state, "--failing", ok_file,
                              "--webhook-env", "FS_TEST_HOOK_UNSET"])
        check(code == 64 and "FS_TEST_HOOK_UNSET is not set" in err,
              "--webhook-env with a variable that is not set exits 64")
        code, out, err = run(["--state", wh_state, "--failing", ok_file,
                              "--webhook-env", "FS_TEST_HOOK",
                              "--webhook", stub.url(secret)])
        check(code == 64 and "not both" in err and "T0K3N" not in err,
              "--webhook and --webhook-env together exit 64")
        port = stub.port
    finally:
        stub.close()
        os.environ.clear()
        os.environ.update(saved_env)
    refused_url = "http://127.0.0.1:%d%s" % (port, secret)
    code, out, err = run(["--state", wh_state, "--failing", live,
                          "--webhook", refused_url, "--apply",
                          "--timeout", "2"])
    check(code == 3 and "could not be reached" in err,
          "a webhook with nothing listening exits 3")
    spill = out + err + read(wh_state)
    check("T0K3N" not in spill and secret not in spill,
          "the webhook path never reaches stdout, stderr or the state file")
    code, out, err = run(["--state", wh_state, "--failing", live,
                          "--webhook", "http://127.0.0.1:%d/bad path" % port,
                          "--apply"])
    check(code == 3 and "bad path" not in err and "failed (" in err,
          "an error that would quote the URL is reduced to its type")

    # ---- a state that cannot be written after the notice
    code, out, err = run(["--state", os.path.join(tmp, "nodir", "s.json"),
                          "--failing", live, "--apply"])
    check(code == 2 and "WAS delivered" in err and "Subject:" in out,
          "a state write that fails after a send says the notice went out")
    code, out, err = run(["--state", os.path.join(tmp, "nodir", "s.json"),
                          "--failing", live, "--apply", "--storm-min", "3"])
    check(code == 2, "and does so on every send")
    cut = write("held-input.txt", "a\nb\nc\nd\n")
    seeded = os.path.join(tmp, "seeded.json")
    save_state(seeded, storm)
    real_save = globals()["save_state"]

    def failing_save(path, state):
        raise OSError("disk full")
    globals()["save_state"] = failing_save
    try:
        code, out, err = run(["--state", seeded, "--failing", cut, "--apply",
                              "--now", stamp(at(1))])
    finally:
        globals()["save_state"] = real_save
    check(code == 2 and "Nothing was held" in err and out == "",
          "a hold whose state cannot be saved exits 2 and says nothing was "
          "held")

    # ---- a corrupt state warns on stderr and still sends
    write("corrupt.json", "{oops")
    code, out, err = run(["--state", os.path.join(tmp, "corrupt.json"),
                          "--failing", live, "--apply"])
    check(code == 0 and "warning: state file" in err and "Subject:" in out
          and json.loads(read(os.path.join(tmp, "corrupt.json")))
          ["version"] == STATE_VERSION,
          "a corrupt state warns, sends, and is replaced by a good one")

    # ---- --now drives a real replay through the CLI
    rs = os.path.join(tmp, "replay.json")
    sent = []
    for hour, names in ((0, "a\nb\nc\n"), (1, "a\nb\nc\nd\n"),
                        (2, "a\nb\nc\nd\n"), (6, "a\nb\nc\nd\n")):
        f = write("r%d.txt" % hour, names)
        code, out, _ = run(["--state", rs, "--failing", f, "--apply",
                            "--now", stamp(at(hour))])
        sent.append(bool(out))
    check(sent == [True, False, False, True],
          "three through the CLI: incident mail, held, held, digest at the "
          "six hour mark")

    # ---- real processes, as cron would run them
    script = os.path.abspath(__file__)
    real_state = os.path.join(tmp, "proc.json")

    def process(*extra):
        proc = subprocess.Popen(
            [sys.executable, script, "--state", real_state, "--failing", live]
            + list(extra), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = proc.communicate()
        return proc.returncode, out, err

    code, out, err = process("--apply")
    check(code == 0 and out.startswith(b"Subject: failstorm: 3 failing")
          and err == b"", "a real process prints the mail on stdout")
    code, out, err = process("--apply")
    check(code == 0 and out == b"" and err == b"",
          "a real second run prints no byte on either stream")
    code, out, err = process("--ap")
    check(code == 64 and out == b"" and b"error" in err,
          "a real run refuses --ap with exit 64")
    proc = subprocess.Popen(
        [sys.executable, script, "--state", real_state, "--failing",
         empty_file], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    proc.communicate()
    check(proc.returncode == 4, "a real run on an empty file exits 4")
    # A name that is not UTF-8 becomes U+FFFD, which a cp1252 pipe cannot
    # write. Without the guard in main() the print raised and no state was
    # saved.
    latin = write("latin.txt", b"caf\xe9\nb\nc\n")
    latin_state = os.path.join(tmp, "latin.json")
    env = dict(os.environ, PYTHONIOENCODING="cp1252")
    proc = subprocess.Popen(
        [sys.executable, script, "--state", latin_state, "--failing", latin,
         "--apply"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
    out, err = proc.communicate()
    check(proc.returncode == 0 and out.startswith(b"Subject: ")
          and os.path.exists(latin_state),
          "a name that the console cannot encode still prints the mail and "
          "saves the state  <-- pinned defect")

    # ---- a crash is 70, never a finding
    real_decide = globals()["decide"]

    def boom(*args, **kwargs):
        raise KeyError("probe")
    globals()["decide"] = boom
    try:
        code, out, err = run(["--state", os.path.join(tmp, "c.json"),
                              "--failing", live])
    finally:
        globals()["decide"] = real_decide
    check(code == 70 and "fault in the tool" in err
          and "Traceback (most recent call last)" in err
          and "KeyError" in err,
          "an unexpected exception exits 70, not a code a wrapper reads as "
          "a finding  <-- pinned defect")
    real_self = globals()["self_test"]
    globals()["self_test"] = lambda: 5
    try:
        check(main(["--self-test"]) == 5,
              "main returns the exit status of --self-test, so a red "
              "self-test fails CI  <-- pinned defect")
    finally:
        globals()["self_test"] = real_self

    # ---- the harness itself, then the footer
    quiet = sys.stdout
    sys.stdout = io.StringIO()
    mark = len(failed)
    try:
        check(False, "probe: a false condition must be recorded")
        raises(lambda: None, "probe: a call that raises nothing must fail")
        raises(lambda: [][0], "probe: the wrong exception must fail")
    finally:
        sys.stdout = quiet
    probe = failed[mark:]
    del failed[mark:]
    check(len(probe) == 3,
          "check() and raises() really do record a failure  <-- pinned "
          "defect")
    raises(lambda: (_ for _ in ()).throw(ValueError("x")),
           "and raises() accepts the exception it is looking for")

    def summary(passed_count, failures):
        print("-" * 68)
        total = passed_count + len(failures)
        if failures:
            print("%d assertions, %d failed" % (total, len(failures)))
            for item in failures:
                print("  FAILED: %s" % item)
            return 1
        print("%d assertions, 0 failed" % total)
        return 0

    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        red = summary(1, ["probe one", "probe two"])
    check(red == 1 and "3 assertions, 2 failed" in buf.getvalue()
          and "  FAILED: probe two" in buf.getvalue(),
          "the footer reports failures by count and by name, and exits 1"
          "  <-- pinned defect")

    spec = importlib.util.spec_from_file_location(
        "failstorm_imported", os.path.abspath(__file__))
    imported = importlib.util.module_from_spec(spec)
    buf = io.StringIO()
    cache_before = sys.dont_write_bytecode
    sys.dont_write_bytecode = True
    try:
        with contextlib.redirect_stdout(buf):
            spec.loader.exec_module(imported)
    finally:
        sys.dont_write_bytecode = cache_before
    check(buf.getvalue() == "" and imported.decide(
        new_state(), {}, at(0)).reason == "ok",
        "importing the module prints nothing and exposes the core")

    return summary(passed[0], failed)


if __name__ == "__main__":
    sys.exit(main())
