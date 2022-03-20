package util

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"

	commcid "github.com/filecoin-project/go-fil-commcid"
	commp "github.com/filecoin-project/go-fil-commp-hashhash"
	"github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	"github.com/mattn/go-isatty"
)

type CarHeader struct {
	Roots   []cid.Cid
	Version uint64
}

func init() {
	cbor.RegisterCborType(CarHeader{})
}

const BufSize = ((4 << 20) / 128 * 127)

func CalculateCommp(reader io.Reader) (commCid cid.Cid) {
	DisableStreamScan := false
	PadPieceSize := uint64(0)

	if isatty.IsTerminal(os.Stdin.Fd()) || isatty.IsCygwinTerminal(os.Stdin.Fd()) {
		log.Println("Reading from STDIN...")
	}

	cp := new(commp.Calc)
	streamBuf := bufio.NewReaderSize(
		io.TeeReader(reader, cp),
		BufSize,
	)

	var streamLen, blockCount int64
	var brokenCar bool
	var carHdr *CarHeader

	if !DisableStreamScan {
		// pretend the stream is a car and try to parse it
		// everything is opportunistic - keep descending on every err == nil
		if maybeHeaderLen, err := streamBuf.Peek(10); err == nil {
			if hdrLen, viLen := binary.Uvarint(maybeHeaderLen); viLen > 0 && hdrLen > 0 {
				actualViLen, err := io.CopyN(ioutil.Discard, streamBuf, int64(viLen))
				streamLen += actualViLen
				if err == nil {
					hdrBuf := make([]byte, hdrLen)
					actualHdrLen, err := io.ReadFull(streamBuf, hdrBuf)
					streamLen += int64(actualHdrLen)
					if err == nil {
						carHdr = new(CarHeader)
						if cbor.DecodeInto(hdrBuf, carHdr) != nil {
							// if it fails - it fails
							carHdr = nil
						} else if carHdr.Version == 1 {
							//
							// I know how to decode this!
							// Warn if we find broken .car-parts
							//
							for {
								maybeNextFrameLen, err := streamBuf.Peek(10)
								if err == io.EOF {
									break
								}
								if err != nil && err != bufio.ErrBufferFull {
									log.Fatalf("unexpected error at offset %d: %s", streamLen, err)
								}
								if len(maybeNextFrameLen) == 0 {
									log.Fatalf("impossible 0-length peek without io.EOF at offset %d", streamLen)
								}

								frameLen, viLen := binary.Uvarint(maybeNextFrameLen)
								if viLen <= 0 {
									// car file with trailing garbage behind it
									log.Printf("aborting car stream parse: undecodeable varint at offset %d", streamLen)
									brokenCar = true
									break
								}
								if frameLen > 2<<20 {
									// anything over ~2MiB got to be a mistake
									log.Printf("aborting car stream parse: unexpectedly large frame length of %d bytes at offset %d", frameLen, streamLen)
									brokenCar = true
									break
								}

								actualFrameLen, err := io.CopyN(ioutil.Discard, streamBuf, int64(viLen)+int64(frameLen))
								streamLen += actualFrameLen
								if err != nil {
									if err != io.EOF {
										log.Fatalf("unexpected error at offset %d: %s", streamLen-actualFrameLen, err)
									}
									log.Printf("aborting car stream parse: truncated frame at offset %d: expected %d bytes but read %d: %s", streamLen-actualFrameLen, frameLen, actualFrameLen, err)
									brokenCar = true
									break
								}
								blockCount++
							}
						}
					}
				}
			}
		}
		// end of "pretend the stream is a car"
	}

	// read out remainder into the hasher, if any
	n, err := io.Copy(ioutil.Discard, streamBuf)
	streamLen += n
	if err != nil && err != io.EOF {
		log.Fatalf("unexpected error at offset %d: %s", streamLen, err)
	}

	rawCommP, paddedSize, err := cp.Digest()
	if err != nil {
		log.Fatal(err)
	}

	if PadPieceSize > 0 {
		rawCommP, err = commp.PadCommP(
			rawCommP,
			paddedSize,
			PadPieceSize,
		)
		if err != nil {
			log.Fatal(err)
		}
		paddedSize = PadPieceSize
	}

	commCid, err = commcid.DataCommitmentV1ToCID(rawCommP)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Fprintf(os.Stderr, `
CommPCid: %s
Payload:        % 12d bytes
Unpadded piece: % 12d bytes
Padded piece:   % 12d bytes
`,
		commCid,
		streamLen,
		paddedSize/128*127,
		paddedSize,
	)

	// we got a header, funny that!
	if carHdr != nil {

		var maybeInvalidText string
		if brokenCar {
			maybeInvalidText = "*CORRUPTED* "
		}

		rootsText := make([]byte, 0, 2048)

		if len(carHdr.Roots) > 0 {
			// rootsText = append(rootsText, '\n')
			for i, c := range carHdr.Roots {
				rootsText = append(
					rootsText,
					fmt.Sprintf("% 5d: %s\n", i+1, c.String())...,
				)
			}
		}

		fmt.Fprintf(os.Stderr, `
%sCARv%d detected in stream:
Blocks:  % 8d
Roots:   % 8d
%s
`,
			maybeInvalidText,
			carHdr.Version,
			blockCount,
			len(carHdr.Roots),
			rootsText,
		)
	}
	return
}