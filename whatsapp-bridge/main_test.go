package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"go.mau.fi/whatsmeow"
	waProto "go.mau.fi/whatsmeow/binary/proto"
	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/proto"
)

// --- Test helpers ---

// mockLIDStore implements store.LIDStore with a simple in-memory map.
type mockLIDStore struct {
	store.NoopStore
	pnByLID map[types.JID]types.JID
	lidByPN map[types.JID]types.JID
}

func (m *mockLIDStore) GetPNForLID(_ context.Context, lid types.JID) (types.JID, error) {
	if pn, ok := m.pnByLID[lid]; ok {
		return pn, nil
	}
	return types.EmptyJID, nil
}

func (m *mockLIDStore) GetLIDForPN(_ context.Context, pn types.JID) (types.JID, error) {
	if lid, ok := m.lidByPN[pn]; ok {
		return lid, nil
	}
	return types.EmptyJID, nil
}

func newTestClient(lidStore store.LIDStore) *whatsmeow.Client {
	noop := &store.NoopStore{}
	return &whatsmeow.Client{
		Store: &store.Device{
			LIDs:     lidStore,
			Contacts: noop,
		},
	}
}

// newTestClientWithSelf builds a test client with the user's own phone JID set
// on Store.ID, which the production code uses as the sender-alt hint for
// outgoing messages. Tests that exercise sender resolution for outgoing
// messages must use this constructor.
func newTestClientWithSelf(lidStore store.LIDStore, selfPhone types.JID) *whatsmeow.Client {
	c := newTestClient(lidStore)
	pn := selfPhone.ToNonAD()
	c.Store.ID = &pn
	return c
}

// querySender returns the sender column for the first message stored under a
// chat JID, or empty string if none.
func querySender(ms *MessageStore, chatJID string) string {
	var s string
	_ = ms.db.QueryRow("SELECT sender FROM messages WHERE chat_jid = ? LIMIT 1", chatJID).Scan(&s)
	return s
}

func newTestMessageStore(t *testing.T) *MessageStore {
	t.Helper()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open in-memory db: %v", err)
	}
	_, err = db.Exec(`
		CREATE TABLE chats (
			jid TEXT PRIMARY KEY,
			name TEXT,
			last_message_time TIMESTAMP
		);
		CREATE TABLE messages (
			id TEXT,
			chat_jid TEXT,
			sender TEXT,
			content TEXT,
			timestamp TIMESTAMP,
			is_from_me BOOLEAN,
			media_type TEXT,
			filename TEXT,
			url TEXT,
			media_key BLOB,
			file_sha256 BLOB,
			file_enc_sha256 BLOB,
			file_length INTEGER,
			PRIMARY KEY (id, chat_jid),
			FOREIGN KEY (chat_jid) REFERENCES chats(jid)
		);
		CREATE TABLE calls (
			call_id TEXT,
			chat_jid TEXT,
			from_jid TEXT,
			timestamp TIMESTAMP,
			is_from_me BOOLEAN,
			call_type TEXT,
			is_group BOOLEAN,
			result TEXT,
			duration_sec INTEGER,
			ended_at TIMESTAMP,
			reason TEXT,
			PRIMARY KEY (call_id, chat_jid)
		);
	`)
	if err != nil {
		t.Fatalf("failed to create tables: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return &MessageStore{db: db}
}

func testLogger() waLog.Logger {
	return waLog.Stdout("Test", "WARN", true)
}

// buildTextMessage constructs an events.Message with the given source fields.
func buildTextMessage(chat, sender, senderAlt, recipientAlt types.JID, isFromMe bool, text string) *events.Message {
	return &events.Message{
		Info: types.MessageInfo{
			MessageSource: types.MessageSource{
				Chat:         chat,
				Sender:       sender,
				SenderAlt:    senderAlt,
				RecipientAlt: recipientAlt,
				IsFromMe:     isFromMe,
				IsGroup:      false,
			},
			ID:        "test-msg-001",
			Timestamp: time.Now(),
		},
		Message: &waProto.Message{
			Conversation: proto.String(text),
		},
	}
}

// queryChat returns the chat JID and name, or empty strings if not found.
func queryChat(ms *MessageStore, jid string) (name string, found bool) {
	err := ms.db.QueryRow("SELECT name FROM chats WHERE jid = ?", jid).Scan(&name)
	return name, err == nil
}

// queryChatLastMessageTime returns the last_message_time for a chat JID.
func queryChatLastMessageTime(ms *MessageStore, jid string) (lastMessageTime string, found bool) {
	err := ms.db.QueryRow("SELECT last_message_time FROM chats WHERE jid = ?", jid).Scan(&lastMessageTime)
	return lastMessageTime, err == nil
}

// queryMessageCount returns the number of messages stored under a chat JID.
func queryMessageCount(ms *MessageStore, chatJID string) int {
	var count int
	_ = ms.db.QueryRow("SELECT COUNT(*) FROM messages WHERE chat_jid = ?", chatJID).Scan(&count)
	return count
}

// --- Test fixtures ---

var (
	phoneLID = types.JID{User: "185366493536339", Server: types.HiddenUserServer}
	phonePN  = types.JID{User: "11234567890", Server: types.DefaultUserServer}
)

// --- Integration tests: handleMessage stores under correct JID ---

func TestHandleMessage_IncomingLIDMessage_StoredUnderPhoneJID(t *testing.T) {
	client := newTestClient(&mockLIDStore{})
	ms := newTestMessageStore(t)
	logger := testLogger()

	msg := buildTextMessage(
		phoneLID,       // chat: arrives as LID
		phoneLID,       // sender: LID
		phonePN,        // senderAlt: phone JID (provided by whatsmeow)
		types.EmptyJID, // recipientAlt: not set for incoming
		false,          // isFromMe: incoming
		"Hola, qué tal?",
	)

	handleMessage(client, ms, msg, logger)

	// Message MUST be stored under the phone-based JID.
	if count := queryMessageCount(ms, phonePN.String()); count != 1 {
		t.Errorf("expected 1 message under phone JID %s, got %d", phonePN, count)
	}

	// No chat entry should exist for the LID JID.
	if _, found := queryChat(ms, phoneLID.String()); found {
		t.Error("LID chat entry should not exist in database")
	}

	// No message should be stored under the LID JID.
	if count := queryMessageCount(ms, phoneLID.String()); count != 0 {
		t.Errorf("expected 0 messages under LID JID %s, got %d", phoneLID, count)
	}
}

func TestHandleMessage_OutgoingLIDMessage_StoredUnderPhoneJID(t *testing.T) {
	client := newTestClient(&mockLIDStore{})
	ms := newTestMessageStore(t)
	logger := testLogger()

	msg := buildTextMessage(
		phoneLID,       // chat: LID
		phoneLID,       // sender: self (LID)
		types.EmptyJID, // senderAlt: not set for outgoing
		phonePN,        // recipientAlt: phone JID
		true,           // isFromMe: outgoing
		"Todo bien!",
	)

	handleMessage(client, ms, msg, logger)

	if count := queryMessageCount(ms, phonePN.String()); count != 1 {
		t.Errorf("expected 1 message under phone JID %s, got %d", phonePN, count)
	}

	if count := queryMessageCount(ms, phoneLID.String()); count != 0 {
		t.Errorf("expected 0 messages under LID JID %s, got %d", phoneLID, count)
	}
}

func TestHandleMessage_LIDWithStoreFallback_StoredUnderPhoneJID(t *testing.T) {
	lidStore := &mockLIDStore{
		pnByLID: map[types.JID]types.JID{phoneLID: phonePN},
	}
	client := newTestClient(lidStore)
	ms := newTestMessageStore(t)
	logger := testLogger()

	// No SenderAlt/RecipientAlt -- must resolve via LID store.
	msg := buildTextMessage(
		phoneLID,       // chat: LID
		phoneLID,       // sender: LID
		types.EmptyJID, // senderAlt: empty (simulates missing alt)
		types.EmptyJID, // recipientAlt: empty
		false,          // isFromMe: incoming
		"Message without alt JIDs",
	)

	handleMessage(client, ms, msg, logger)

	if count := queryMessageCount(ms, phonePN.String()); count != 1 {
		t.Errorf("expected 1 message under phone JID %s, got %d", phonePN, count)
	}

	if count := queryMessageCount(ms, phoneLID.String()); count != 0 {
		t.Errorf("expected 0 messages under LID JID %s, got %d", phoneLID, count)
	}
}

func TestHandleMessage_PhoneJID_Unaffected(t *testing.T) {
	client := newTestClient(&mockLIDStore{})
	ms := newTestMessageStore(t)
	logger := testLogger()

	msg := buildTextMessage(
		phonePN,        // chat: already phone-based
		phonePN,        // sender: phone-based
		types.EmptyJID, // senderAlt: empty
		types.EmptyJID, // recipientAlt: empty
		false,          // isFromMe: incoming
		"Normal message",
	)

	handleMessage(client, ms, msg, logger)

	if count := queryMessageCount(ms, phonePN.String()); count != 1 {
		t.Errorf("expected 1 message under phone JID %s, got %d", phonePN, count)
	}
}

// --- Sender column resolution ---
//
// These tests guard against the regression where the bridge stored the
// LID user-part (or, for outgoing messages, the recipient's phone) in the
// sender column even after the chat-JID was resolved to a phone JID.

var (
	selfLID   = types.JID{User: "999888777666555", Server: types.HiddenUserServer}
	selfPhone = types.JID{User: "10000000000", Server: types.DefaultUserServer}
)

// TestHandleMessage_OutgoingFromSelf_SenderIsOwnPhone asserts that an
// outgoing message from a LID-typed self does not get the recipient's
// phone written into the sender column. Before the fix, resolveLIDChat
// reused recipientAlt for the sender, mis-attributing self messages.
func TestHandleMessage_OutgoingFromSelf_SenderIsOwnPhone(t *testing.T) {
	client := newTestClientWithSelf(&mockLIDStore{}, selfPhone)
	ms := newTestMessageStore(t)
	logger := testLogger()

	msg := buildTextMessage(
		phoneLID,       // chat: peer LID
		selfLID,        // sender: own LID
		types.EmptyJID, // senderAlt: empty for outgoing
		phonePN,        // recipientAlt: peer phone (NOT self phone)
		true,           // outgoing
		"hi",
	)

	handleMessage(client, ms, msg, logger)

	got := querySender(ms, phonePN.String())
	if got != selfPhone.User {
		t.Errorf("outgoing sender = %q, want own phone user %q (recipient phone is %q, must not appear)",
			got, selfPhone.User, phonePN.User)
	}
}

// TestHandleMessage_IncomingLID_SenderResolvedFromAlt asserts that an
// incoming LID-only sender with a non-empty SenderAlt is rewritten to the
// peer's phone user-part, not stored as the raw LID number.
func TestHandleMessage_IncomingLID_SenderResolvedFromAlt(t *testing.T) {
	client := newTestClient(&mockLIDStore{})
	ms := newTestMessageStore(t)
	logger := testLogger()

	msg := buildTextMessage(
		phoneLID,       // chat: LID
		phoneLID,       // sender: peer LID
		phonePN,        // senderAlt: peer phone
		types.EmptyJID, // recipientAlt: unused for incoming
		false,          // incoming
		"hola",
	)

	handleMessage(client, ms, msg, logger)

	got := querySender(ms, phonePN.String())
	if got != phonePN.User {
		t.Errorf("incoming sender = %q, want peer phone user %q", got, phonePN.User)
	}
}

// TestHandleMessage_IncomingLID_SenderResolvedFromStore covers the
// history-sync-style case: SenderAlt is empty but the LID store has a
// PN mapping for the peer LID, so the sender column should still end up
// as the phone user-part.
func TestHandleMessage_IncomingLID_SenderResolvedFromStore(t *testing.T) {
	client := newTestClient(&mockLIDStore{
		pnByLID: map[types.JID]types.JID{phoneLID: phonePN},
	})
	ms := newTestMessageStore(t)
	logger := testLogger()

	msg := buildTextMessage(
		phoneLID,       // chat: LID
		phoneLID,       // sender: peer LID
		types.EmptyJID, // senderAlt: empty (post-fix, fallback to LID store)
		types.EmptyJID, // recipientAlt: empty
		false,          // incoming
		"hello",
	)

	handleMessage(client, ms, msg, logger)

	got := querySender(ms, phonePN.String())
	if got != phonePN.User {
		t.Errorf("incoming sender = %q, want peer phone user %q (LID store fallback)",
			got, phonePN.User)
	}
}

// TestHandleMessage_LIDWithoutMapping_SenderFallsBackToLID asserts the
// graceful-degradation path: with no SenderAlt and no LID store mapping,
// the bridge stores the raw LID user-part rather than failing or writing
// an unrelated value.
func TestHandleMessage_LIDWithoutMapping_SenderFallsBackToLID(t *testing.T) {
	client := newTestClient(&mockLIDStore{})
	ms := newTestMessageStore(t)
	logger := testLogger()

	msg := buildTextMessage(
		phoneLID,       // chat: LID
		phoneLID,       // sender: peer LID
		types.EmptyJID, // senderAlt: empty
		types.EmptyJID, // recipientAlt: empty
		false,          // incoming
		"orphan",
	)

	handleMessage(client, ms, msg, logger)

	// Chat JID has no mapping either, so the message ends up under the LID chat.
	got := querySender(ms, phoneLID.String())
	if got != phoneLID.User {
		t.Errorf("orphan-LID sender = %q, want raw LID user %q (graceful fallback)",
			got, phoneLID.User)
	}
}

// --- LID sender backfill migration ---

func TestMigrateLegacyLIDSendersToPhones_RewritesAndIsIdempotent(t *testing.T) {
	ms := newTestMessageStore(t)
	logger := testLogger()

	tmpDir := t.TempDir()
	whatsappDBPath := filepath.Join(tmpDir, "whatsapp.db")

	waDB, err := sql.Open("sqlite3", whatsappDBPath)
	if err != nil {
		t.Fatalf("failed to create whatsapp db: %v", err)
	}
	defer func() { _ = waDB.Close() }()

	if _, err := waDB.Exec(`
		CREATE TABLE whatsmeow_lid_map (
			lid TEXT PRIMARY KEY,
			pn TEXT NOT NULL
		);
		INSERT INTO whatsmeow_lid_map (lid, pn) VALUES ('111', '222');
		INSERT INTO whatsmeow_lid_map (lid, pn) VALUES ('333', '444');
	`); err != nil {
		t.Fatalf("failed to prepare lid map db: %v", err)
	}

	chatPhone := "222@s.whatsapp.net"
	groupChat := "group@g.us"

	if _, err := ms.db.Exec(`
		INSERT INTO chats (jid, name, last_message_time) VALUES
			(?, 'Peer', '2026-03-01T10:00:00Z'),
			(?, 'Group', '2026-03-01T11:00:00Z');

		INSERT INTO messages (id, chat_jid, sender, content, timestamp, is_from_me, media_type, filename, url, media_key, file_sha256, file_enc_sha256, file_length) VALUES
			('m1', ?, '111', 'incoming dm pre-fix',  '2026-03-01T10:00:00Z', 0, '', '', '', NULL, NULL, NULL, 0),
			('m2', ?, '222', 'incoming dm post-fix', '2026-03-01T10:01:00Z', 0, '', '', '', NULL, NULL, NULL, 0),
			('g1', ?, '333', 'group msg pre-fix',    '2026-03-01T11:00:00Z', 0, '', '', '', NULL, NULL, NULL, 0),
			('g2', ?, '999', 'group msg unmapped',   '2026-03-01T11:01:00Z', 0, '', '', '', NULL, NULL, NULL, 0);
	`, chatPhone, groupChat, chatPhone, chatPhone, groupChat, groupChat); err != nil {
		t.Fatalf("failed to seed message store: %v", err)
	}

	if err := ms.MigrateLegacyLIDSendersToPhones(whatsappDBPath, logger); err != nil {
		t.Fatalf("migration failed: %v", err)
	}

	type row struct {
		id, sender string
	}
	var got []row
	rows, err := ms.db.Query("SELECT id, sender FROM messages ORDER BY id")
	if err != nil {
		t.Fatalf("failed to read messages: %v", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.id, &r.sender); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, r)
	}

	want := map[string]string{
		"m1": "222", // rewritten via lid map
		"m2": "222", // already phone, untouched
		"g1": "444", // rewritten via lid map
		"g2": "999", // unmapped LID stays as-is (graceful fallback)
	}
	for _, r := range got {
		if w, ok := want[r.id]; !ok || r.sender != w {
			t.Errorf("message %s: sender = %q, want %q", r.id, r.sender, w)
		}
	}

	if err := ms.MigrateLegacyLIDSendersToPhones(whatsappDBPath, logger); err != nil {
		t.Fatalf("second run should be no-op, got error: %v", err)
	}
}

func TestMigrateLegacyLIDSendersToPhones_MissingWhatsAppDBIsNoOp(t *testing.T) {
	ms := newTestMessageStore(t)
	logger := testLogger()

	missingPath := filepath.Join(t.TempDir(), "missing-whatsapp.db")
	if err := ms.MigrateLegacyLIDSendersToPhones(missingPath, logger); err != nil {
		t.Fatalf("expected missing whatsapp db to be a no-op, got error: %v", err)
	}
}

// TestHandleMessage_GroupParticipantLID_ResolvedViaStore covers the
// highest-volume path that triggers the LID-sender bug: a group message
// where the participant JID is LID-only and the per-message SenderAlt is
// empty. Resolution must come from the LID store.
func TestHandleMessage_GroupParticipantLID_ResolvedViaStore(t *testing.T) {
	groupJID := types.JID{User: "254110094043-1619359480", Server: types.GroupServer}
	participantLID := types.JID{User: "261391827087520", Server: types.HiddenUserServer}
	participantPhone := types.JID{User: "31612345678", Server: types.DefaultUserServer}

	client := newTestClient(&mockLIDStore{
		pnByLID: map[types.JID]types.JID{participantLID: participantPhone},
	})
	ms := newTestMessageStore(t)
	logger := testLogger()

	// Pre-seed the group chat row so GetChatName short-circuits on the
	// existing-name path and doesn't try to issue a GetGroupInfo IQ
	// against the fake client.
	if err := ms.StoreChat(groupJID.String(), "Test Group", time.Now()); err != nil {
		t.Fatalf("seed group chat: %v", err)
	}

	msg := &events.Message{
		Info: types.MessageInfo{
			MessageSource: types.MessageSource{
				Chat:     groupJID,
				Sender:   participantLID,
				IsFromMe: false,
				IsGroup:  true,
			},
			ID:        "test-group-001",
			Timestamp: time.Now(),
		},
		Message: &waProto.Message{
			Conversation: proto.String("group hello"),
		},
	}

	handleMessage(client, ms, msg, logger)

	got := querySender(ms, groupJID.String())
	if got != participantPhone.User {
		t.Errorf("group participant sender = %q, want phone user %q", got, participantPhone.User)
	}
}

// TestHandleHistorySync_LIDParticipant_ResolvedViaStore exercises the
// history-sync code path. Because history-sync rows do not carry SenderAlt,
// resolution must succeed via the LID store fallback that
// resolveUserJID consults. The stored sender column must be the phone
// user-part, not the raw LID number copied verbatim from Key.Participant.
func TestHandleHistorySync_LIDParticipant_ResolvedViaStore(t *testing.T) {
	chatJID := phonePN.String() // history-sync conversation already keyed by phone
	participantLID := types.JID{User: "445566778899", Server: types.HiddenUserServer}
	participantPhone := types.JID{User: "11234567890", Server: types.DefaultUserServer}

	client := newTestClientWithSelf(&mockLIDStore{
		pnByLID: map[types.JID]types.JID{participantLID: participantPhone},
	}, selfPhone)
	ms := newTestMessageStore(t)
	logger := testLogger()

	historySync := &events.HistorySync{
		Data: &waProto.HistorySync{
			SyncType: waProto.HistorySync_RECENT.Enum(),
			Conversations: []*waProto.Conversation{
				{
					ID: proto.String(chatJID),
					Messages: []*waProto.HistorySyncMsg{
						{
							Message: &waProto.WebMessageInfo{
								Key: &waCommon.MessageKey{
									ID:          proto.String("hist-msg-001"),
									FromMe:      proto.Bool(false),
									Participant: proto.String(participantLID.String()),
								},
								MessageTimestamp: proto.Uint64(uint64(time.Now().Unix())),
								Message: &waProto.Message{
									Conversation: proto.String("history payload"),
								},
							},
						},
					},
				},
			},
		},
	}

	handleHistorySync(client, ms, historySync, logger)

	got := querySender(ms, chatJID)
	if got != participantPhone.User {
		t.Errorf("history-sync sender = %q, want resolved phone user %q (raw LID was %q)",
			got, participantPhone.User, participantLID.User)
	}
}

func TestMigrateLegacyLIDChatsToPhoneJIDs_MigratesAndIsIdempotent(t *testing.T) {
	ms := newTestMessageStore(t)
	logger := testLogger()

	tmpDir := t.TempDir()
	whatsappDBPath := filepath.Join(tmpDir, "whatsapp.db")

	waDB, err := sql.Open("sqlite3", whatsappDBPath)
	if err != nil {
		t.Fatalf("failed to create whatsapp db: %v", err)
	}
	defer func() { _ = waDB.Close() }()

	if _, err := waDB.Exec(`
		CREATE TABLE whatsmeow_lid_map (
			lid TEXT PRIMARY KEY,
			pn TEXT NOT NULL
		);
		INSERT INTO whatsmeow_lid_map (lid, pn) VALUES ('111', '222');
	`); err != nil {
		t.Fatalf("failed to prepare lid map db: %v", err)
	}

	lidJID := "111@lid"
	phoneJID := "222@s.whatsapp.net"

	_, err = ms.db.Exec(`
		INSERT INTO chats (jid, name, last_message_time) VALUES
			(?, 'Legacy LID Name', '2026-03-01T10:00:00Z'),
			(?, '', '2026-03-01T09:00:00Z');

		INSERT INTO messages (id, chat_jid, sender, content, timestamp, is_from_me, media_type, filename, url, media_key, file_sha256, file_enc_sha256, file_length) VALUES
			('dup', ?, 'alice', 'lid duplicate', '2026-03-01T10:00:00Z', 0, '', '', '', NULL, NULL, NULL, 0),
			('only-lid', ?, 'alice', 'lid only', '2026-03-01T10:01:00Z', 0, '', '', '', NULL, NULL, NULL, 0),
			('dup', ?, 'alice', 'phone duplicate', '2026-03-01T10:00:00Z', 0, '', '', '', NULL, NULL, NULL, 0),
			('only-phone', ?, 'alice', 'phone only', '2026-03-01T10:02:00Z', 0, '', '', '', NULL, NULL, NULL, 0);
	`, lidJID, phoneJID, lidJID, lidJID, phoneJID, phoneJID)
	if err != nil {
		t.Fatalf("failed to seed message store: %v", err)
	}

	if err := ms.MigrateLegacyLIDChatsToPhoneJIDs(whatsappDBPath, logger); err != nil {
		t.Fatalf("migration failed: %v", err)
	}

	if lidCount := queryMessageCount(ms, lidJID); lidCount != 0 {
		t.Fatalf("expected 0 messages under migrated LID chat, got %d", lidCount)
	}
	if phoneCount := queryMessageCount(ms, phoneJID); phoneCount != 3 {
		t.Fatalf("expected 3 messages under phone chat after dedupe, got %d", phoneCount)
	}

	if _, found := queryChat(ms, lidJID); found {
		t.Fatalf("expected migrated LID chat row to be removed")
	}

	phoneName, found := queryChat(ms, phoneJID)
	if !found {
		t.Fatalf("expected phone chat row to exist after migration")
	}
	if phoneName != "Legacy LID Name" {
		t.Fatalf("expected phone chat name to be hydrated from LID chat, got %q", phoneName)
	}

	phoneTime, timeFound := queryChatLastMessageTime(ms, phoneJID)
	if !timeFound {
		t.Fatalf("expected phone chat to have last_message_time after migration")
	}
	if phoneTime != "2026-03-01T10:00:00Z" {
		t.Fatalf("expected phone chat last_message_time to be the latest (from LID chat), got %q", phoneTime)
	}

	if err := ms.MigrateLegacyLIDChatsToPhoneJIDs(whatsappDBPath, logger); err != nil {
		t.Fatalf("second migration run should be a no-op, got error: %v", err)
	}
	if phoneCount := queryMessageCount(ms, phoneJID); phoneCount != 3 {
		t.Fatalf("expected idempotent result with 3 phone messages, got %d", phoneCount)
	}
}

func TestMigrateLegacyLIDChatsToPhoneJIDs_MissingWhatsAppDBIsNoOp(t *testing.T) {
	ms := newTestMessageStore(t)
	logger := testLogger()

	missingPath := filepath.Join(t.TempDir(), "missing-whatsapp.db")
	if err := ms.MigrateLegacyLIDChatsToPhoneJIDs(missingPath, logger); err != nil {
		t.Fatalf("expected missing whatsapp db to be treated as no-op, got error: %v", err)
	}
}

func TestExtractTextContent_SurfacesMediaCaptions(t *testing.T) {
	cases := []struct {
		name string
		msg  *waProto.Message
		want string
	}{
		{
			name: "Conversation",
			msg:  &waProto.Message{Conversation: proto.String("hola")},
			want: "hola",
		},
		{
			name: "ExtendedTextMessage",
			msg: &waProto.Message{
				ExtendedTextMessage: &waProto.ExtendedTextMessage{Text: proto.String("quoted reply")},
			},
			want: "quoted reply",
		},
		{
			name: "ImageMessage with caption",
			msg: &waProto.Message{
				ImageMessage: &waProto.ImageMessage{Caption: proto.String("sunset on the beach")},
			},
			want: "sunset on the beach",
		},
		{
			name: "VideoMessage with caption",
			msg: &waProto.Message{
				VideoMessage: &waProto.VideoMessage{Caption: proto.String("the kids playing")},
			},
			want: "the kids playing",
		},
		{
			name: "DocumentMessage with caption",
			msg: &waProto.Message{
				DocumentMessage: &waProto.DocumentMessage{Caption: proto.String("invoice attached")},
			},
			want: "invoice attached",
		},
		{
			name: "ImageMessage without caption returns empty",
			msg:  &waProto.Message{ImageMessage: &waProto.ImageMessage{}},
			want: "",
		},
		{
			name: "Nil message returns empty",
			msg:  nil,
			want: "",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := extractTextContent(tc.msg)
			if got != tc.want {
				t.Errorf("extractTextContent() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestMigrateLegacyLIDChatsToPhoneJIDs_AggregatesByPhoneJIDDeterministically(t *testing.T) {
	ms := newTestMessageStore(t)
	logger := testLogger()

	tmpDir := t.TempDir()
	whatsappDBPath := filepath.Join(tmpDir, "whatsapp.db")

	waDB, err := sql.Open("sqlite3", whatsappDBPath)
	if err != nil {
		t.Fatalf("failed to create whatsapp db: %v", err)
	}
	defer func() { _ = waDB.Close() }()

	if _, err := waDB.Exec(`
		CREATE TABLE whatsmeow_lid_map (
			lid TEXT PRIMARY KEY,
			pn TEXT NOT NULL
		);
		INSERT INTO whatsmeow_lid_map (lid, pn) VALUES ('111', '222');
		INSERT INTO whatsmeow_lid_map (lid, pn) VALUES ('333', '222');
	`); err != nil {
		t.Fatalf("failed to prepare lid map db: %v", err)
	}

	lidA := "111@lid"
	lidB := "333@lid"
	phoneJID := "222@s.whatsapp.net"

	_, err = ms.db.Exec(`
		INSERT INTO chats (jid, name, last_message_time) VALUES
			(?, 'Older Name', '2026-03-01T10:00:00Z'),
			(?, 'Newest Name', '2026-03-01T11:00:00Z');

		INSERT INTO messages (id, chat_jid, sender, content, timestamp, is_from_me, media_type, filename, url, media_key, file_sha256, file_enc_sha256, file_length) VALUES
			('a1', ?, 'alice', 'from lid A', '2026-03-01T10:00:00Z', 0, '', '', '', NULL, NULL, NULL, 0),
			('b1', ?, 'bob', 'from lid B', '2026-03-01T11:00:00Z', 0, '', '', '', NULL, NULL, NULL, 0);
	`, lidA, lidB, lidA, lidB)
	if err != nil {
		t.Fatalf("failed to seed message store: %v", err)
	}

	if err := ms.MigrateLegacyLIDChatsToPhoneJIDs(whatsappDBPath, logger); err != nil {
		t.Fatalf("migration failed: %v", err)
	}

	if count := queryMessageCount(ms, lidA); count != 0 {
		t.Fatalf("expected no messages under first LID after migration, got %d", count)
	}
	if count := queryMessageCount(ms, lidB); count != 0 {
		t.Fatalf("expected no messages under second LID after migration, got %d", count)
	}
	if count := queryMessageCount(ms, phoneJID); count != 2 {
		t.Fatalf("expected 2 messages under phone JID after migration, got %d", count)
	}

	name, found := queryChat(ms, phoneJID)
	if !found {
		t.Fatalf("expected merged phone chat row to exist")
	}
	if name != "Newest Name" {
		t.Fatalf("expected deterministic name selection from latest source chat, got %q", name)
	}

	var lastMessage string
	if err := ms.db.QueryRow("SELECT last_message_time FROM chats WHERE jid = ?", phoneJID).Scan(&lastMessage); err != nil {
		t.Fatalf("failed to read merged last_message_time: %v", err)
	}
	if lastMessage != "2026-03-01T11:00:00Z" {
		t.Fatalf("expected merged last_message_time to be max source value, got %s", lastMessage)
	}
}

// buildImageMessage constructs an events.Message that carries an ImageMessage
// with no download metadata (URL/media-key are empty), so handleMessage will
// classify it as an image but skip the synchronous download attempt.
func buildImageMessage(chat, sender types.JID, isFromMe bool, caption string) *events.Message {
	img := &waProto.ImageMessage{}
	if caption != "" {
		img.Caption = proto.String(caption)
	}
	return &events.Message{
		Info: types.MessageInfo{
			MessageSource: types.MessageSource{
				Chat:     chat,
				Sender:   sender,
				IsFromMe: isFromMe,
			},
			ID:        "test-img-001",
			Timestamp: time.Now(),
		},
		Message: &waProto.Message{ImageMessage: img},
	}
}

// captureWebhook starts a local httptest server that records the first webhook
// payload it receives. It returns the server and a channel that yields the
// decoded payload.
func captureWebhook(t *testing.T) (*httptest.Server, <-chan WebhookPayload) {
	t.Helper()
	ch := make(chan WebhookPayload, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var p WebhookPayload
		if err := json.Unmarshal(body, &p); err == nil {
			select {
			case ch <- p:
			default:
			}
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	return srv, ch
}

// TestHandleMessage_ImageOnly_WebhookForwarded verifies that an image message
// with no text caption is forwarded to the webhook endpoint (not silently
// dropped), and that the webhook payload contains the expected media fields.
func TestHandleMessage_ImageOnly_WebhookForwarded(t *testing.T) {
	srv, webhookCh := captureWebhook(t)
	t.Setenv("WEBHOOK_URL", srv.URL)

	client := newTestClient(&mockLIDStore{})
	ms := newTestMessageStore(t)
	logger := testLogger()

	msg := buildImageMessage(phonePN, phonePN, false, "") // no caption

	handleMessage(client, ms, msg, logger)

	// The image-only message must be stored.
	if count := queryMessageCount(ms, phonePN.String()); count != 1 {
		t.Errorf("expected 1 message stored, got %d", count)
	}

	// The webhook must have been called.
	select {
	case payload := <-webhookCh:
		if payload.MediaType != "image" {
			t.Errorf("expected mediaType=image, got %q", payload.MediaType)
		}
		if payload.MessageID != "test-img-001" {
			t.Errorf("expected messageId=test-img-001, got %q", payload.MessageID)
		}
		if payload.Content != "" {
			t.Errorf("expected empty content for image-only message, got %q", payload.Content)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for webhook call")
	}
}

// TestHandleMessage_ImageWithCaption_WebhookForwarded verifies that an image
// message WITH a text caption is forwarded and that the caption is included in
// the webhook content field (extractTextContent now surfaces image captions).
func TestHandleMessage_ImageWithCaption_WebhookForwarded(t *testing.T) {
	srv, webhookCh := captureWebhook(t)
	t.Setenv("WEBHOOK_URL", srv.URL)

	client := newTestClient(&mockLIDStore{})
	ms := newTestMessageStore(t)
	logger := testLogger()

	msg := buildImageMessage(phonePN, phonePN, false, "look at this!")

	handleMessage(client, ms, msg, logger)

	select {
	case payload := <-webhookCh:
		if payload.MediaType != "image" {
			t.Errorf("expected mediaType=image, got %q", payload.MediaType)
		}
		if payload.MessageID != "test-img-001" {
			t.Errorf("expected messageId=test-img-001, got %q", payload.MessageID)
		}
		if payload.Content != "look at this!" {
			t.Errorf("expected caption in content, got %q", payload.Content)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for webhook call")
	}
}

// queryCallResult returns the (result, duration_sec, reason) for a call row,
// or empties if no row exists.
func queryCallResult(ms *MessageStore, callID, chatJID string) (result string, duration sql.NullInt64, reason sql.NullString, found bool) {
	err := ms.db.QueryRow(
		"SELECT result, duration_sec, reason FROM calls WHERE call_id = ? AND chat_jid = ?",
		callID, chatJID,
	).Scan(&result, &duration, &reason)
	return result, duration, reason, err == nil
}

// TestCallStateMachine_AllTransitions exercises every documented transition of
// the call lifecycle state machine and pins down the non-obvious invariants:
//
//   - Offer → Accept → Terminate          ⇒ "ended" (with computed duration)
//   - Offer → Terminate (no Accept)       ⇒ "missed"
//   - Offer → Reject → Terminate          ⇒ "rejected" is preserved
//     (Terminate's CASE branch must NOT downgrade rejected to ended/missed)
//   - Duplicate Offer events do not clobber a call already in a later state
//   - MarkCallAnswered/Rejected only fire when row is still in_progress
func TestCallStateMachine_AllTransitions(t *testing.T) {
	type step struct {
		name string
		do   func(ms *MessageStore) error
	}

	t0 := time.Date(2026, 4, 22, 10, 0, 0, 0, time.UTC)
	t30 := t0.Add(30 * time.Second)
	t90 := t0.Add(90 * time.Second)

	cases := []struct {
		name         string
		callID       string
		chatJID      string
		steps        []step
		wantResult   string
		wantDuration int64 // 0 = expect NULL
		wantReason   string
	}{
		{
			name:    "Offer→Accept→Terminate yields ended with duration",
			callID:  "call-answered",
			chatJID: "creator@s.whatsapp.net",
			steps: []step{
				{"offer", func(ms *MessageStore) error {
					return ms.StoreCallOffer("call-answered", "creator@s.whatsapp.net", "creator@s.whatsapp.net", t0, false, "voice", false)
				}},
				{"accept", func(ms *MessageStore) error {
					return ms.MarkCallAnswered("call-answered", "creator@s.whatsapp.net")
				}},
				{"terminate", func(ms *MessageStore) error {
					return ms.MarkCallTerminated("call-answered", "creator@s.whatsapp.net", "normal", t90)
				}},
			},
			wantResult:   "ended",
			wantDuration: 90,
			wantReason:   "normal",
		},
		{
			name:    "Offer→Terminate with no Accept yields missed",
			callID:  "call-missed",
			chatJID: "creator@s.whatsapp.net",
			steps: []step{
				{"offer", func(ms *MessageStore) error {
					return ms.StoreCallOffer("call-missed", "creator@s.whatsapp.net", "creator@s.whatsapp.net", t0, false, "voice", false)
				}},
				{"terminate", func(ms *MessageStore) error {
					return ms.MarkCallTerminated("call-missed", "creator@s.whatsapp.net", "timeout", t30)
				}},
			},
			wantResult:   "missed",
			wantDuration: 30,
			wantReason:   "timeout",
		},
		{
			name:    "Offer→Reject→Terminate preserves rejected",
			callID:  "call-rejected",
			chatJID: "creator@s.whatsapp.net",
			steps: []step{
				{"offer", func(ms *MessageStore) error {
					return ms.StoreCallOffer("call-rejected", "creator@s.whatsapp.net", "creator@s.whatsapp.net", t0, false, "voice", false)
				}},
				{"reject", func(ms *MessageStore) error {
					return ms.MarkCallRejected("call-rejected", "creator@s.whatsapp.net")
				}},
				{"terminate", func(ms *MessageStore) error {
					return ms.MarkCallTerminated("call-rejected", "creator@s.whatsapp.net", "rejected_by_user", t30)
				}},
			},
			wantResult:   "rejected",
			wantDuration: 30,
			wantReason:   "rejected_by_user",
		},
		{
			name:    "Duplicate Offer does not clobber later state",
			callID:  "call-dup-offer",
			chatJID: "creator@s.whatsapp.net",
			steps: []step{
				{"offer", func(ms *MessageStore) error {
					return ms.StoreCallOffer("call-dup-offer", "creator@s.whatsapp.net", "creator@s.whatsapp.net", t0, false, "voice", false)
				}},
				{"accept", func(ms *MessageStore) error {
					return ms.MarkCallAnswered("call-dup-offer", "creator@s.whatsapp.net")
				}},
				{"duplicate offer (should be ignored)", func(ms *MessageStore) error {
					return ms.StoreCallOffer("call-dup-offer", "creator@s.whatsapp.net", "creator@s.whatsapp.net", t0, false, "voice", false)
				}},
				{"terminate", func(ms *MessageStore) error {
					return ms.MarkCallTerminated("call-dup-offer", "creator@s.whatsapp.net", "normal", t90)
				}},
			},
			wantResult:   "ended",
			wantDuration: 90,
			wantReason:   "normal",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ms := newTestMessageStore(t)
			for _, s := range tc.steps {
				if err := s.do(ms); err != nil {
					t.Fatalf("step %q failed: %v", s.name, err)
				}
			}

			result, duration, reason, found := queryCallResult(ms, tc.callID, tc.chatJID)
			if !found {
				t.Fatalf("expected row for call_id=%s chat_jid=%s, got none", tc.callID, tc.chatJID)
			}
			if result != tc.wantResult {
				t.Errorf("result: got %q, want %q", result, tc.wantResult)
			}
			if !duration.Valid || duration.Int64 != tc.wantDuration {
				t.Errorf("duration_sec: got %v, want %d", duration, tc.wantDuration)
			}
			if !reason.Valid || reason.String != tc.wantReason {
				t.Errorf("reason: got %v, want %q", reason, tc.wantReason)
			}
		})
	}
}

// TestCallStateMachine_AcceptAndRejectAreNoOpAfterTerminate verifies that
// late-arriving Accept/Reject events (post-Terminate) do not corrupt a
// finalized row. The WHERE result='in_progress' guard is what enforces this.
func TestCallStateMachine_AcceptAndRejectAreNoOpAfterTerminate(t *testing.T) {
	ms := newTestMessageStore(t)
	t0 := time.Date(2026, 4, 22, 10, 0, 0, 0, time.UTC)

	if err := ms.StoreCallOffer("call-late", "creator@s.whatsapp.net", "creator@s.whatsapp.net", t0, false, "voice", false); err != nil {
		t.Fatalf("offer: %v", err)
	}
	if err := ms.MarkCallTerminated("call-late", "creator@s.whatsapp.net", "timeout", t0.Add(30*time.Second)); err != nil {
		t.Fatalf("terminate: %v", err)
	}

	// These should be no-ops because the row is already 'missed', not 'in_progress'.
	_ = ms.MarkCallAnswered("call-late", "creator@s.whatsapp.net")
	_ = ms.MarkCallRejected("call-late", "creator@s.whatsapp.net")

	result, _, _, _ := queryCallResult(ms, "call-late", "creator@s.whatsapp.net")
	if result != "missed" {
		t.Errorf("expected missed to be preserved, got %q", result)
	}
}

// TestCallChatJID_Precedence pins down the precedence rules in callChatJID:
//
//  1. GroupJID wins (group calls always key on the group)
//  2. CallCreator wins over From (the bug Ed fixed: Accept events arrive
//     with From=accepter's JID, which is "us" if user picked up on phone)
//  3. From is the last-resort fallback
//
// Without rule 2, Accept UPDATEs miss the row stored at Offer time and the
// state machine falls through to "missed" when the user answered elsewhere.
func TestCallChatJID_Precedence(t *testing.T) {
	groupJID := types.JID{User: "120363012345678901", Server: types.GroupServer}
	creatorJID := types.JID{User: "11234567890", Server: types.DefaultUserServer}
	fromJID := types.JID{User: "19998887777", Server: types.DefaultUserServer}

	cases := []struct {
		name string
		meta types.BasicCallMeta
		want string
	}{
		{
			name: "group JID wins when present",
			meta: types.BasicCallMeta{
				GroupJID:    groupJID,
				CallCreator: creatorJID,
				From:        fromJID,
			},
			want: groupJID.String(),
		},
		{
			name: "creator wins over From for 1:1 (Accept-from-other-device case)",
			meta: types.BasicCallMeta{
				CallCreator: creatorJID,
				From:        fromJID,
			},
			want: creatorJID.ToNonAD().String(),
		},
		{
			name: "From is fallback when creator is empty",
			meta: types.BasicCallMeta{
				From: fromJID,
			},
			want: fromJID.ToNonAD().String(),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := callChatJID(tc.meta)
			if got != tc.want {
				t.Errorf("callChatJID() = %q, want %q", got, tc.want)
			}
		})
	}
}

// --- Outbound persistence: storeOutgoingMessage (#674) ---
//
// Messages the bridge sends through /api/send are never echoed back to this
// device, so handleMessage never sees them. These tests pin the outbound
// twin that writes them, and check it files a sent message the same way an
// equivalent received message would be filed.

// outgoingRow is one messages row, read back for assertions.
type outgoingRow struct {
	sender    string
	content   string
	isFromMe  bool
	mediaType string
	filename  string
	url       string
	mediaKey  []byte
}

// queryOutgoingRow returns the row stored under a message ID.
func queryOutgoingRow(t *testing.T, ms *MessageStore, msgID string) (outgoingRow, string) {
	t.Helper()
	var r outgoingRow
	var chatJID string
	err := ms.db.QueryRow(
		"SELECT chat_jid, sender, content, is_from_me, media_type, filename, url, media_key FROM messages WHERE id = ?",
		msgID,
	).Scan(&chatJID, &r.sender, &r.content, &r.isFromMe, &r.mediaType, &r.filename, &r.url, &r.mediaKey)
	if err != nil {
		t.Fatalf("no message row for id %q: %v", msgID, err)
	}
	return r, chatJID
}

// textToSend builds the proto message sendWhatsAppMessage builds for a plain
// text send.
func textToSend(text string) *waProto.Message {
	return &waProto.Message{Conversation: proto.String(text)}
}

// imageToSend builds the proto message sendWhatsAppMessage builds for an image
// send, carrying the upload fields the CDN download path later needs.
func imageToSend(caption string) *waProto.Message {
	length := uint64(4096)
	return &waProto.Message{
		ImageMessage: &waProto.ImageMessage{
			Caption:       proto.String(caption),
			Mimetype:      proto.String("image/jpeg"),
			URL:           proto.String("https://mmg.whatsapp.net/outgoing"),
			MediaKey:      []byte("outgoing-media-key"),
			FileSHA256:    []byte("sha"),
			FileEncSHA256: []byte("encsha"),
			FileLength:    &length,
		},
	}
}

// A send addressed to a LID must be stored under the phone JID, so a chat's
// sent and received messages share one chat_jid.
func TestStoreOutgoingMessage_LIDRecipient_StoredUnderPhoneJID(t *testing.T) {
	client := newTestClientWithSelf(&mockLIDStore{}, phonePN)
	ms := newTestMessageStore(t)

	err := storeOutgoingMessage(client, ms, phoneLID, phonePN, "out-001",
		textToSend("Map digest 27 Jul"), time.Now(), testLogger())
	if err != nil {
		t.Fatalf("storeOutgoingMessage returned error: %v", err)
	}

	row, chatJID := queryOutgoingRow(t, ms, "out-001")
	if chatJID != phonePN.String() {
		t.Errorf("expected chat_jid %s, got %s", phonePN, chatJID)
	}
	if !row.isFromMe {
		t.Error("expected is_from_me to be true for a bridge send")
	}
	if row.content != "Map digest 27 Jul" {
		t.Errorf("expected content to be the sent text, got %q", row.content)
	}
	if count := queryMessageCount(ms, phoneLID.String()); count != 0 {
		t.Errorf("expected 0 messages under LID JID %s, got %d", phoneLID, count)
	}
}

// The sender column must carry our own phone user-part, matching what
// handleMessage stores for an outgoing message echoed from another device.
func TestStoreOutgoingMessage_SenderIsOwnPhone(t *testing.T) {
	self := types.JID{User: "447854069173", Server: types.DefaultUserServer}
	client := newTestClientWithSelf(&mockLIDStore{}, self)
	ms := newTestMessageStore(t)

	if err := storeOutgoingMessage(client, ms, phoneLID, phonePN, "out-002",
		textToSend("hello"), time.Now(), testLogger()); err != nil {
		t.Fatalf("storeOutgoingMessage returned error: %v", err)
	}

	row, _ := queryOutgoingRow(t, ms, "out-002")
	if row.sender != self.User {
		t.Errorf("expected sender %s (own phone), got %s", self.User, row.sender)
	}
}

// A send addressed to a plain phone JID passes through unchanged.
func TestStoreOutgoingMessage_PhoneRecipient_Unaffected(t *testing.T) {
	client := newTestClientWithSelf(&mockLIDStore{}, phonePN)
	ms := newTestMessageStore(t)

	if err := storeOutgoingMessage(client, ms, phonePN, phonePN, "out-003",
		textToSend("plain"), time.Now(), testLogger()); err != nil {
		t.Fatalf("storeOutgoingMessage returned error: %v", err)
	}

	_, chatJID := queryOutgoingRow(t, ms, "out-003")
	if chatJID != phonePN.String() {
		t.Errorf("expected chat_jid %s, got %s", phonePN, chatJID)
	}
}

// When the recipient hint is itself a LID, the whatsmeow LID store resolves
// the chat — the same fallback the inbound path uses.
func TestStoreOutgoingMessage_LIDWithStoreFallback(t *testing.T) {
	lidStore := &mockLIDStore{pnByLID: map[types.JID]types.JID{phoneLID: phonePN}}
	client := newTestClientWithSelf(lidStore, phonePN)
	ms := newTestMessageStore(t)

	if err := storeOutgoingMessage(client, ms, phoneLID, phoneLID, "out-004",
		textToSend("via store"), time.Now(), testLogger()); err != nil {
		t.Fatalf("storeOutgoingMessage returned error: %v", err)
	}

	_, chatJID := queryOutgoingRow(t, ms, "out-004")
	if chatJID != phonePN.String() {
		t.Errorf("expected chat_jid %s from LID store, got %s", phonePN, chatJID)
	}
}

// A media send stores the caption as content and the upload fields as media
// columns, so the media stays downloadable from the store afterwards.
func TestStoreOutgoingMessage_MediaColumnsPersisted(t *testing.T) {
	client := newTestClientWithSelf(&mockLIDStore{}, phonePN)
	ms := newTestMessageStore(t)

	if err := storeOutgoingMessage(client, ms, phonePN, phonePN, "out-005",
		imageToSend("chart for John"), time.Now(), testLogger()); err != nil {
		t.Fatalf("storeOutgoingMessage returned error: %v", err)
	}

	row, _ := queryOutgoingRow(t, ms, "out-005")
	if row.content != "chart for John" {
		t.Errorf("expected the caption as content, got %q", row.content)
	}
	if row.mediaType != "image" {
		t.Errorf("expected media_type image, got %q", row.mediaType)
	}
	if row.filename == "" {
		t.Error("expected a filename to be generated for the sent image")
	}
	if row.url != "https://mmg.whatsapp.net/outgoing" {
		t.Errorf("expected the upload URL to be stored, got %q", row.url)
	}
	if string(row.mediaKey) != "outgoing-media-key" {
		t.Errorf("expected the media key to be stored, got %q", row.mediaKey)
	}
}

// Sending must move the chat's last_message_time forward, as receiving does.
func TestStoreOutgoingMessage_UpdatesChatLastMessageTime(t *testing.T) {
	client := newTestClientWithSelf(&mockLIDStore{}, phonePN)
	ms := newTestMessageStore(t)

	earlier := time.Now().Add(-time.Hour)
	if err := ms.StoreChat(phonePN.String(), "Ben", earlier); err != nil {
		t.Fatalf("failed to seed chat: %v", err)
	}
	before, _ := queryChatLastMessageTime(ms, phonePN.String())

	if err := storeOutgoingMessage(client, ms, phonePN, phonePN, "out-006",
		textToSend("later"), time.Now(), testLogger()); err != nil {
		t.Fatalf("storeOutgoingMessage returned error: %v", err)
	}

	after, found := queryChatLastMessageTime(ms, phonePN.String())
	if !found {
		t.Fatal("chat row missing after send")
	}
	if after == before {
		t.Error("expected last_message_time to move forward after a send")
	}
	// The existing chat name must survive the send.
	if name, _ := queryChat(ms, phonePN.String()); name != "Ben" {
		t.Errorf("expected chat name Ben to be preserved, got %q", name)
	}
}

// A send with neither text nor media writes nothing and reports no error.
func TestStoreOutgoingMessage_EmptyMessageNotStored(t *testing.T) {
	client := newTestClientWithSelf(&mockLIDStore{}, phonePN)
	ms := newTestMessageStore(t)

	if err := storeOutgoingMessage(client, ms, phonePN, phonePN, "out-007",
		textToSend(""), time.Now(), testLogger()); err != nil {
		t.Fatalf("expected no error for an empty message, got %v", err)
	}

	if count := queryMessageCount(ms, phonePN.String()); count != 0 {
		t.Errorf("expected 0 messages stored for an empty send, got %d", count)
	}
}

// A missing message ID is reported rather than written as a blank-keyed row.
func TestStoreOutgoingMessage_MissingIDIsReported(t *testing.T) {
	client := newTestClientWithSelf(&mockLIDStore{}, phonePN)
	ms := newTestMessageStore(t)

	if err := storeOutgoingMessage(client, ms, phonePN, phonePN, "",
		textToSend("no id"), time.Now(), testLogger()); err == nil {
		t.Error("expected an error when the send returned no message ID")
	}
	if count := queryMessageCount(ms, phonePN.String()); count != 0 {
		t.Errorf("expected nothing stored without a message ID, got %d rows", count)
	}
}

// TestStoreOutgoingMessage_WritesToRealMessagesDB runs the outbound write
// against a real on-disk messages.db built by NewMessageStore, not the
// hand-copied schema newTestMessageStore uses. It is the disk-level proof
// that a bridge send lands as a row in the production schema, and it catches
// drift between the two schemas.
func TestStoreOutgoingMessage_WritesToRealMessagesDB(t *testing.T) {
	t.Chdir(t.TempDir())

	ms, err := NewMessageStore()
	if err != nil {
		t.Fatalf("NewMessageStore failed: %v", err)
	}
	t.Cleanup(func() { _ = ms.Close() })

	sent := time.Now().UTC().Truncate(time.Second)
	if err := storeOutgoingMessage(newTestClientWithSelf(&mockLIDStore{}, phonePN), ms,
		phoneLID, phonePN, "disk-001", textToSend("outbound on the record"), sent, testLogger()); err != nil {
		t.Fatalf("storeOutgoingMessage returned error: %v", err)
	}

	if _, statErr := os.Stat(filepath.Join("store", "messages.db")); statErr != nil {
		t.Fatalf("expected store/messages.db on disk: %v", statErr)
	}

	// Read back through a second connection to the same file, so the
	// assertion cannot pass on in-process state alone.
	db, err := sql.Open("sqlite3", "file:store/messages.db?mode=ro")
	if err != nil {
		t.Fatalf("failed to reopen messages.db: %v", err)
	}
	defer func() { _ = db.Close() }()

	var chatJID, sender, content string
	var isFromMe bool
	if err := db.QueryRow(
		"SELECT chat_jid, sender, content, is_from_me FROM messages WHERE id = ?", "disk-001",
	).Scan(&chatJID, &sender, &content, &isFromMe); err != nil {
		t.Fatalf("sent message not found in messages.db: %v", err)
	}

	if chatJID != phonePN.String() {
		t.Errorf("expected chat_jid %s, got %s", phonePN, chatJID)
	}
	if sender != phonePN.User {
		t.Errorf("expected sender %s, got %s", phonePN.User, sender)
	}
	if content != "outbound on the record" {
		t.Errorf("expected the sent text as content, got %q", content)
	}
	if !isFromMe {
		t.Error("expected is_from_me to be true")
	}
}

// A first send to a contact the store has never seen must name the chat after
// the recipient. GetChatName's last-resort name is the user-part it is handed,
// and for an outbound message the sender is us, so handing it the sender would
// name the chat after our own number.
func TestStoreOutgoingMessage_NewChatNamedAfterRecipient(t *testing.T) {
	self := types.JID{User: "447854069173", Server: types.DefaultUserServer}
	client := newTestClientWithSelf(&mockLIDStore{}, self)
	ms := newTestMessageStore(t)

	if err := storeOutgoingMessage(client, ms, phoneLID, phonePN, "out-008",
		textToSend("first contact"), time.Now(), testLogger()); err != nil {
		t.Fatalf("storeOutgoingMessage returned error: %v", err)
	}

	name, found := queryChat(ms, phonePN.String())
	if !found {
		t.Fatal("expected a chat row for the recipient")
	}
	if name == self.User {
		t.Errorf("chat named after our own number %s, expected the recipient", self.User)
	}
	if name != phonePN.User {
		t.Errorf("expected chat name %s (the recipient), got %q", phonePN.User, name)
	}
}

// An account whose own Store.ID is a LID must still store the phone user-part
// as the sender, resolved through the LID store.
func TestStoreOutgoingMessage_OwnLIDSenderResolvedViaStore(t *testing.T) {
	ownLID := types.JID{User: "999888777666555", Server: types.HiddenUserServer}
	ownPN := types.JID{User: "447854069173", Server: types.DefaultUserServer}
	lidStore := &mockLIDStore{pnByLID: map[types.JID]types.JID{ownLID: ownPN}}

	client := newTestClient(lidStore)
	id := ownLID.ToNonAD()
	client.Store.ID = &id
	ms := newTestMessageStore(t)

	if err := storeOutgoingMessage(client, ms, phonePN, phonePN, "out-009",
		textToSend("from a LID account"), time.Now(), testLogger()); err != nil {
		t.Fatalf("storeOutgoingMessage returned error: %v", err)
	}

	row, _ := queryOutgoingRow(t, ms, "out-009")
	if row.sender != ownPN.User {
		t.Errorf("expected sender %s resolved from the LID store, got %s", ownPN.User, row.sender)
	}
}

// An account whose own Store.ID is a LID the store cannot map keeps the LID
// user-part rather than storing an empty sender.
func TestStoreOutgoingMessage_OwnLIDWithoutMappingKeepsLID(t *testing.T) {
	ownLID := types.JID{User: "999888777666555", Server: types.HiddenUserServer}
	client := newTestClient(&mockLIDStore{})
	id := ownLID.ToNonAD()
	client.Store.ID = &id
	ms := newTestMessageStore(t)

	if err := storeOutgoingMessage(client, ms, phonePN, phonePN, "out-010",
		textToSend("unmapped LID account"), time.Now(), testLogger()); err != nil {
		t.Fatalf("storeOutgoingMessage returned error: %v", err)
	}

	row, _ := queryOutgoingRow(t, ms, "out-010")
	if row.sender != ownLID.User {
		t.Errorf("expected sender to fall back to the LID user-part %s, got %s", ownLID.User, row.sender)
	}
}

// A group send stores under the group JID untouched — the LID rewrite that
// applies to 1:1 recipients must not touch a group. The chat is seeded with a
// name so GetChatName short-circuits and the test needs no connection.
func TestStoreOutgoingMessage_GroupSendStoredUnderGroupJID(t *testing.T) {
	group := types.JID{User: "120363151143532472", Server: types.GroupServer}
	client := newTestClientWithSelf(&mockLIDStore{}, phonePN)
	ms := newTestMessageStore(t)

	if err := ms.StoreChat(group.String(), "Family", time.Now().Add(-time.Hour)); err != nil {
		t.Fatalf("failed to seed group chat: %v", err)
	}

	if err := storeOutgoingMessage(client, ms, group, group, "out-011",
		textToSend("group digest"), time.Now(), testLogger()); err != nil {
		t.Fatalf("storeOutgoingMessage returned error: %v", err)
	}

	row, chatJID := queryOutgoingRow(t, ms, "out-011")
	if chatJID != group.String() {
		t.Errorf("expected chat_jid %s, got %s", group, chatJID)
	}
	if !row.isFromMe {
		t.Error("expected is_from_me to be true for a group send")
	}
	if row.sender != phonePN.User {
		t.Errorf("expected sender %s, got %s", phonePN.User, row.sender)
	}
	if name, _ := queryChat(ms, group.String()); name != "Family" {
		t.Errorf("expected the group name Family to be preserved, got %q", name)
	}
}

// A send to a group the store has never seen must not make a network request.
// GetGroupInfo carries whatsmeow's 75-second request timeout, which outlives
// the 60-second HTTP write timeout on /api/send, so a delivered group message
// could return a failure to the caller and invite a duplicate send.
//
// The client is nil on purpose, as a tripwire: if this path ever regains a
// client call, it will panic here rather than pass quietly. That is less than
// proof of absence — the group branch returns before touching the client at
// all, so the nil is never dereferenced either way, and it relies on whatsmeow
// methods panicking on a nil receiver. A name assertion alone would prove even
// less, because GetChatName on a disconnected client also falls back to a
// generated name.
func TestStoreOutgoingMessage_UnseededGroup_MakesNoNetworkLookup(t *testing.T) {
	group := types.JID{User: "120363151143532472", Server: types.GroupServer}
	ms := newTestMessageStore(t)

	if err := storeOutgoingMessage(nil, ms, group, group, "out-012",
		textToSend("first group send"), time.Now(), testLogger()); err != nil {
		t.Fatalf("storeOutgoingMessage returned error: %v", err)
	}

	row, chatJID := queryOutgoingRow(t, ms, "out-012")
	if chatJID != group.String() {
		t.Errorf("expected chat_jid %s, got %s", group, chatJID)
	}
	if !row.isFromMe {
		t.Error("expected is_from_me to be true for a group send")
	}

	// The name is left empty so a later inbound message can fill it in.
	name, found := queryChat(ms, group.String())
	if !found {
		t.Fatal("expected a chat row for the group")
	}
	if name != "" {
		t.Errorf("expected an empty group name pending an inbound message, got %q", name)
	}
}

// outgoingChatName keeps a group name that is already stored, and the empty
// name a first group send leaves behind does not block a later one from being
// written. That is the contract this test covers, and all it covers: it writes
// the second name through StoreChat rather than through an inbound message, so
// it says nothing about what an inbound message would resolve the name to.
//
// It is also why the send stores an empty name rather than a generated one.
// GetChatName keeps an existing name only when it is non-empty, so a generated
// name would be permanent. An empty one can still be replaced — though not
// necessarily by the real name: if the first inbound message's group-info
// lookup fails, GetChatName writes its own "Group <id>" fallback, and that
// then sticks.
func TestStoreOutgoingMessage_KeepsExistingGroupName(t *testing.T) {
	group := types.JID{User: "120363151143532472", Server: types.GroupServer}
	ms := newTestMessageStore(t)

	if err := storeOutgoingMessage(nil, ms, group, group, "out-013",
		textToSend("first group send"), time.Now(), testLogger()); err != nil {
		t.Fatalf("storeOutgoingMessage returned error: %v", err)
	}
	if name, _ := queryChat(ms, group.String()); name != "" {
		t.Fatalf("expected an empty group name after the send, got %q", name)
	}

	// A resolved name reaches the chats row. Written directly here, not
	// through handleMessage — this test does not exercise the inbound path.
	if err := ms.StoreChat(group.String(), "Family", time.Now()); err != nil {
		t.Fatalf("failed to store the resolved chat name: %v", err)
	}

	client := newTestClient(&mockLIDStore{})
	if name := outgoingChatName(client, ms, group, group.String(), testLogger()); name != "Family" {
		t.Errorf("expected the stored name Family to be kept, got %q", name)
	}
}

// --- Wiring: sendWhatsAppMessage must call the store (#674) ---

// stubDelivery stands in for the whatsmeow client's delivery of a message, so
// the rest of sendWhatsAppMessage — including the store write — runs without a
// WhatsApp connection. It reports the given message ID and time, or the given
// error. It holds no package state, so it is safe under parallel tests.
type stubDelivery struct {
	connected bool
	id        string
	sentAt    time.Time
	err       error

	sentTo  types.JID
	sentMsg *waProto.Message
	calls   int
}

func (s *stubDelivery) IsConnected() bool { return s.connected }

func (s *stubDelivery) SendMessage(_ context.Context, to types.JID, msg *waProto.Message,
	_ ...whatsmeow.SendRequestExtra) (whatsmeow.SendResponse, error) {
	s.calls++
	s.sentTo = to
	s.sentMsg = msg
	if s.err != nil {
		return whatsmeow.SendResponse{}, s.err
	}
	return whatsmeow.SendResponse{ID: s.id, Timestamp: s.sentAt}, nil
}

// deliversAs returns a stub that reports a successful send.
func deliversAs(msgID string, sentAt time.Time) *stubDelivery {
	return &stubDelivery{connected: true, id: msgID, sentAt: sentAt}
}

// This is the regression guard for #674 itself: it fails if sendWhatsAppMessage
// ever stops writing what it sent into the store. Every other test in this
// group calls storeOutgoingMessage directly and would stay green if the two
// were disconnected.
func TestSendWhatsAppMessage_StoresTheSentMessage(t *testing.T) {
	// The delivery seam is a parameter, not package state, so this is safe.
	t.Parallel()
	sentAt := time.Now().Truncate(time.Second)
	wa := deliversAs("wired-001", sentAt)

	lidStore := &mockLIDStore{pnByLID: map[types.JID]types.JID{phoneLID: phonePN}}
	client := newTestClientWithSelf(lidStore, phonePN)
	ms := newTestMessageStore(t)

	// Address the LID directly, so the send path's own phone-to-LID lookup —
	// which would reach the network — is skipped.
	ok, msg := sendWhatsAppMessage(client, wa, ms, phoneLID.String(), "wired through to the store", "", testLogger())
	if !ok {
		t.Fatalf("expected the send to succeed, got %q", msg)
	}

	row, chatJID := queryOutgoingRow(t, ms, "wired-001")
	if chatJID != phonePN.String() {
		t.Errorf("expected chat_jid %s, got %s", phonePN, chatJID)
	}
	if row.content != "wired through to the store" {
		t.Errorf("expected the sent text as content, got %q", row.content)
	}
	if !row.isFromMe {
		t.Error("expected is_from_me to be true")
	}
	if row.sender != phonePN.User {
		t.Errorf("expected sender %s, got %s", phonePN.User, row.sender)
	}
	if wa.calls != 1 {
		t.Errorf("expected exactly one delivery, got %d", wa.calls)
	}
}

// A send that fails must not leave a row behind.
func TestSendWhatsAppMessage_FailedSendStoresNothing(t *testing.T) {
	// The delivery seam is a parameter, not package state, so this is safe.
	t.Parallel()
	wa := &stubDelivery{connected: true, err: io.ErrUnexpectedEOF}
	client := newTestClientWithSelf(&mockLIDStore{}, phonePN)
	ms := newTestMessageStore(t)

	if ok, _ := sendWhatsAppMessage(client, wa, ms, phoneLID.String(), "never delivered", "", testLogger()); ok {
		t.Fatal("expected the send to report failure")
	}
	if count := queryMessageCount(ms, phonePN.String()); count != 0 {
		t.Errorf("expected nothing stored for a failed send, got %d rows", count)
	}
}

// A store failure must not turn a delivered message into a failed send.
func TestSendWhatsAppMessage_StoreFailureStillReportsSuccess(t *testing.T) {
	// The delivery seam is a parameter, not package state, so this is safe.
	t.Parallel()
	wa := deliversAs("wired-002", time.Now())

	client := newTestClientWithSelf(&mockLIDStore{}, phonePN)
	ms := newTestMessageStore(t)
	// Close the database so every write fails.
	if err := ms.db.Close(); err != nil {
		t.Fatalf("failed to close the test database: %v", err)
	}

	ok, msg := sendWhatsAppMessage(client, wa, ms, phoneLID.String(), "delivered but unstorable", "", testLogger())
	if !ok {
		t.Errorf("expected a delivered message to report success despite the store failure, got %q", msg)
	}
}

// --- Media-retry participant JID (#131) ---

// A group media-retry receipt must name the sender by the LID the group addresses
// them with. The messages table stores a bare phone user-part, so the LID has to be
// recovered from the LID-PN store.
func TestRetryParticipantJID_BarePhoneResolvesToLID(t *testing.T) {
	client := newTestClient(&mockLIDStore{
		lidByPN: map[types.JID]types.JID{phonePN: phoneLID},
	})

	got := retryParticipantJID(client, phonePN.User)

	if got != phoneLID {
		t.Errorf("participant JID = %q, want peer LID %q", got.String(), phoneLID.String())
	}
}

// Without a LID mapping the participant must still be a routable phone JID. A bare
// user-part is not: types.ParseJID puts it in the server slot and returns no error,
// which encodes as a JID pair with an empty user that the server cannot route.
func TestRetryParticipantJID_NoLIDMappingFallsBackToPhoneJID(t *testing.T) {
	client := newTestClient(&mockLIDStore{})

	got := retryParticipantJID(client, phonePN.User)

	if got != phonePN {
		t.Errorf("participant JID = %q, want phone JID %q", got.String(), phonePN.String())
	}
}

// A sender that already carries a server is passed through the same LID lookup, so
// a legacy row storing a full JID resolves the same way as a bare user-part.
func TestRetryParticipantJID_QualifiedPhoneJIDResolvesToLID(t *testing.T) {
	client := newTestClient(&mockLIDStore{
		lidByPN: map[types.JID]types.JID{phonePN: phoneLID},
	})

	got := retryParticipantJID(client, phonePN.String())

	if got != phoneLID {
		t.Errorf("participant JID = %q, want peer LID %q", got.String(), phoneLID.String())
	}
}
