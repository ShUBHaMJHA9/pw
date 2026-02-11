#!/usr/bin/env python3
"""
Quick DB inspection CLI for testing the PW database.

Usage examples:
  python db.py --list-batches
  python db.py --list-subjects --batch BATCH_ID
  python db.py --list-chapters --batch BATCH_ID --subject SUBJECT_SLUG
  python db.py --list-lectures --batch BATCH_ID --subject SUBJECT_SLUG --chapter "Chapter Name"
  python db.py --lecture-info --batch BATCH_ID --lecture-id LECTURE_ID

This script uses `mainLogic.utils.mysql_logger` connection helpers.
"""
import argparse
import os
import sys
from pprint import pprint

try:
	from mainLogic.utils import mysql_logger as db
except Exception as e:
	print(f"Failed to import mysql_logger: {e}")
	sys.exit(1)


def _connect():
	# use the module's init if PWDL_DB_URL not set
	try:
		return db._connect()
	except Exception:
		# Try to init from env and retry
		try:
			db.init(None)
			return db._connect()
		except Exception as e:
			print(f"DB connect failed: {e}")
			sys.exit(1)


def list_batches(conn):
	cur = conn.cursor()
	# Prefer courses table, fallback to lecture_jobs
	cur.execute("SELECT id, batch_id, batch_slug, name FROM courses ORDER BY id")
	rows = cur.fetchall()
	if rows:
		print("Batches (from courses):")
		for r in rows:
			print(f"- id={r.get('id')} batch_id={r.get('batch_id')} slug={r.get('batch_slug')} name={r.get('name')}")
		return
	cur.execute("SELECT DISTINCT batch_id FROM lecture_jobs ORDER BY batch_id")
	rows = cur.fetchall()
	print("Batches (from lecture_jobs):")
	for r in rows:
		print(f"- batch_id={r.get('batch_id')}")


def list_subjects(conn, batch_id=None):
	cur = conn.cursor()
	if batch_id:
		cur.execute(
			"SELECT s.id, s.slug, s.name FROM subjects s JOIN courses c ON c.id = s.course_id WHERE c.batch_id=%s ORDER BY s.name",
			(batch_id,),
		)
	else:
		cur.execute("SELECT id, slug, name FROM subjects ORDER BY name")
	rows = cur.fetchall()
	print("Subjects:")
	for r in rows:
		print(f"- id={r.get('id')} slug={r.get('slug')} name={r.get('name')}")


def list_chapters(conn, subject_slug=None, batch_id=None):
	cur = conn.cursor()
	if subject_slug:
		cur.execute(
			"SELECT ch.id, ch.name, s.slug FROM chapters ch JOIN subjects s ON s.id = ch.subject_id WHERE s.slug=%s ORDER BY ch.name",
			(subject_slug,),
		)
	elif batch_id:
		cur.execute(
			"SELECT ch.id, ch.name, s.slug FROM chapters ch JOIN subjects s ON s.id = ch.subject_id JOIN courses c ON c.id = s.course_id WHERE c.batch_id=%s ORDER BY s.slug, ch.name",
			(batch_id,),
		)
	else:
		cur.execute("SELECT id, name FROM chapters ORDER BY name")
	rows = cur.fetchall()
	print("Chapters:")
	for r in rows:
		print(f"- id={r.get('id')} subject={r.get('slug') or 'N/A'} name={r.get('name')}")


def list_lectures(conn, batch_id=None, subject_slug=None, chapter_name=None):
	cur = conn.cursor()
	sql = (
		"SELECT l.batch_id, l.lecture_id, l.display_order, l.chapter_total, l.lecture_name, l.chapter_name, l.start_time, u.status, u.telegram_chat_id, u.telegram_message_id, u.file_path "
		"FROM lectures l LEFT JOIN lecture_uploads u ON l.batch_id=u.batch_id AND l.lecture_id=u.lecture_id"
	)
	cond = []
	params = []
	if batch_id:
		cond.append("l.batch_id=%s")
		params.append(batch_id)
	if subject_slug:
		cond.append("l.subject_slug=%s")
		params.append(subject_slug)
	if chapter_name:
		cond.append("l.chapter_name=%s")
		params.append(chapter_name)
	if cond:
		sql += " WHERE " + " AND ".join(cond)
	sql += " ORDER BY l.start_time DESC LIMIT 500"
	cur.execute(sql, tuple(params))
	rows = cur.fetchall()
	print("Lectures:")
	for r in rows:
		name = (r.get('lecture_name') or '')[:80]
		chapter = (r.get('chapter_name') or '')[:40]
		order = r.get('display_order')
		total = r.get('chapter_total')
		ord_text = f"#{order}/{total} " if order and total else (f"#{order} " if order else "")
		print(
			f"- lecture_id={r.get('lecture_id')} {ord_text}name={name} chapter={chapter} start={r.get('start_time')} status={r.get('status')} chat_id={r.get('telegram_chat_id')} msg_id={r.get('telegram_message_id')} file={r.get('file_path')}"
		)


def lecture_info(conn, batch_id, lecture_id):
	cur = conn.cursor()
	cur.execute(
		"SELECT l.*, u.status, u.telegram_chat_id, u.telegram_message_id, u.file_path FROM lectures l LEFT JOIN lecture_uploads u ON l.batch_id=u.batch_id AND l.lecture_id=u.lecture_id WHERE l.batch_id=%s AND l.lecture_id=%s",
		(batch_id, lecture_id),
	)
	row = cur.fetchone()
	if not row:
		print("Lecture not found.")
		return
	pprint(row)


def main():
	parser = argparse.ArgumentParser(description="Inspect PWDB (test-only).")
	parser.add_argument("--db-url", help="Override DB URL (mysql://user:pass@host/db)")
	parser.add_argument("--list-batches", action="store_true")
	parser.add_argument("--list-subjects", action="store_true")
	parser.add_argument("--list-chapters", action="store_true")
	parser.add_argument("--list-lectures", action="store_true")
	parser.add_argument("--lecture-info", action="store_true")
	parser.add_argument("--batch", help="Batch ID to filter")
	parser.add_argument("--subject", help="Subject slug to filter")
	parser.add_argument("--chapter", help="Chapter name to filter")
	parser.add_argument("--lecture-id", help="Lecture ID to inspect")
	args = parser.parse_args()

	if args.db_url:
		os.environ["PWDL_DB_URL"] = args.db_url
	try:
		db.init(None)
	except Exception as e:
		print(f"DB init failed: {e}")
		# continue, _connect will try

	conn = _connect()
	try:
		if args.list_batches:
			list_batches(conn)
		elif args.list_subjects:
			list_subjects(conn, batch_id=args.batch)
		elif args.list_chapters:
			list_chapters(conn, subject_slug=args.subject, batch_id=args.batch)
		elif args.list_lectures:
			list_lectures(conn, batch_id=args.batch, subject_slug=args.subject, chapter_name=args.chapter)
		elif args.lecture_info and args.batch and args.lecture_id:
			lecture_info(conn, args.batch, args.lecture_id)
		else:
			parser.print_help()
	finally:
		try:
			conn.close()
		except Exception:
			pass


if __name__ == "__main__":
	main()

