# Release Smoke Test

## Clean-machine style pass

1. Start the app:

```bash
cd /path/to/storagex
./run.sh
```

2. Open `http://127.0.0.1:8000`.
3. Save a valid Google OAuth client ID and client secret in `Settings`.
4. Connect the YouTube account.
5. Upload one small `.txt` file.
6. Confirm the file appears in the library.
7. Create a folder, upload into it, then confirm the file appears in that folder.
8. Double-click the file name, rename it inline, and confirm the new name stays visible.
9. Reload the page and confirm the library stays visible while state refreshes.
10. Download the file and confirm the recovered bytes match the source and the suggested filename uses the local rename.
11. Try a wrong key and confirm the recovery finishes with failed integrity.
12. Restart the app and confirm YouTube credentials and local folder organization reload, but the encryption key does not.
13. Disconnect YouTube and confirm the app returns to the disconnected state.
14. Use `Reset local YouTube setup` and confirm the saved local auth/config is wiped.

## Error-path pass

- Save a mismatched client ID / client secret and confirm the callback error is actionable.
- Use an OAuth app without the right redirect URI and confirm the redirect mismatch message is actionable.
- Use a Google account that is not a test user and confirm the access-denied message is actionable.
- Try recovery without the right browser cookies and confirm the error tells the user to sign into YouTube in Chrome or Safari on the same Mac.
