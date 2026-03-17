# ⚠️  DO NOT COMMIT THIS FILE WHILE THE MAP IS EMPTY  ⚠️
#
# An empty checksum map shipped inside a Hex package causes every downstream
# user (who does not set force_build: true) to get a hard error when Mix tries
# to verify the downloaded precompiled NIF artifact.  Keep this file out of
# version control until it has been fully populated for the release in question.
#
# How to populate:
#   1. Create and push the vX.Y.Z git tag.
#   2. Wait for the GitHub Actions "Release" workflow to finish uploading all
#      precompiled NIF artifacts to the GitHub release page.
#   3. Run:
#
#        mix rustler_precompiled.download ExArrow.Native --all
#
#      This fetches every target/OTP combination and writes the SHA-256
#      digests into this file.
#   4. Verify the map is non-empty, then commit the file on the release branch.
#   5. Run `mix hex.publish`.  The file is listed in mix.exs :files so it is
#      bundled in the Hex package even though .gitignore excludes it from SCM.
#
# During local development config/config.exs forces force_build: true for the
# :ex_arrow app, so this file is never consulted and may safely be absent.
%{
}
