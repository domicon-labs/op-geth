package kzg_sdk

import (
	"crypto/ecdsa"
	"crypto/rand"
	"fmt"
	"math/big"
	"os"

	"github.com/consensys/gnark-crypto/ecc"
	bls12381 "github.com/consensys/gnark-crypto/ecc/bls12-381"
	"github.com/consensys/gnark-crypto/ecc/bls12-381/fr"
	"github.com/consensys/gnark-crypto/ecc/bls12-381/fr/kzg"
	"github.com/domicon-labs/op-geth/common"
	"github.com/domicon-labs/op-geth/common/math"
	"github.com/domicon-labs/op-geth/crypto"
	"github.com/domicon-labs/op-geth/crypto/secp256k1"
	solsha3 "github.com/miguelmota/go-solidity-sha3"
)

const dChunkSize = 30
const dSrsSize = 1 << 16

type DomiconSdk struct {
	srs *kzg.SRS
}

// NewDomiconSdk 初始化sdk，可以设置srsSize=1 << 16
func GenerateSRSFile(srsSize uint64) error {
	quickSrs, err := kzg.NewSRS(ecc.NextPowerOfTwo(srsSize), big.NewInt(-1))
	if err != nil {
		fmt.Println("NewSRS failed, ", err)
		return err
	}
	file, err := os.Create("./srs")
	if err != nil {
		fmt.Println("create file failed, ", err)
		return err
	}
	defer file.Close()
	quickSrs.WriteTo(file)
	if err != nil {
		fmt.Println("write file failed, ", err)
		return err
	}
	return nil
}

// all user should load same srs file
func InitDomiconSdk(srsSize uint64, srsPath string) (*DomiconSdk, error) {
	var newsrs kzg.SRS
	newsrs.Pk.G1 = make([]bls12381.G1Affine, srsSize)
	if _, err := os.Stat(srsPath); err != nil {
		return nil, err
	}
	file, err := os.Open(srsPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	_, err = newsrs.ReadFrom(file)
	if err != nil {
		return nil, err
	}

	return &DomiconSdk{srs: &newsrs}, nil
}

func (domiconSdk *DomiconSdk) GenerateDataCommit(data []byte) (kzg.Digest, error) {
	poly := dataToPolynomial(data)
	digest, err := kzg.Commit(poly, domiconSdk.srs.Pk)
	if err != nil {
		return kzg.Digest{}, err
	}
	return digest, nil
}

func dataToPolynomial(data []byte) []fr.Element {
	chunks := chunkBytes(data, dChunkSize)
	chunksLen := len(chunks)

	ps := make([]fr.Element, chunksLen)
	for i, chunk := range chunks {
		ps[i].SetBytes(chunk)
	}
	return ps
}

func (domiconSdk *DomiconSdk) DataCommit(polynomials []fr.Element) (kzg.Digest, error) {
	digest, err := kzg.Commit(polynomials, domiconSdk.srs.Pk)
	return digest, err
}

func (domiconSdk *DomiconSdk) TxSign(key *ecdsa.PrivateKey, commitment kzg.Digest, addressA common.Address, addressB common.Address, data []byte) ([]byte, []byte) {
	commitmentBytes := commitment.Bytes()
	var mergedData []byte
	mergedData = append(mergedData, commitmentBytes[:]...)
	mergedData = append(mergedData, addressA.Bytes()...)
	mergedData = append(mergedData, addressB.Bytes()...)
	mergedData = append(mergedData, data...)

	return sign(string(mergedData), key)
}

func chunkBytes(data []byte, chunkSize int) [][]byte {
	var chunks [][]byte

	for i := 0; i < len(data); i += chunkSize {
		end := i + chunkSize
		if end > len(data) {
			end = len(data)
		}
		chunks = append(chunks, data[i:end])
	}

	return chunks
}

func eyGen() *ecdsa.PrivateKey {
	key, err := ecdsa.GenerateKey(crypto.S256(), rand.Reader)

	if err != nil {
		panic(err)
	}

	return key
}

func sign(message string, key *ecdsa.PrivateKey) ([]byte, []byte) {
	// Turn the message into a 32-byte hash
	hash := solsha3.SoliditySHA3(solsha3.String(message))
	// Prefix and then hash to mimic behavior of eth_sign
	prefixed := solsha3.SoliditySHA3(solsha3.String("\x19Ethereum Signed Message:\n32"), solsha3.Bytes32(hash))
	sig, err := secp256k1.Sign(prefixed, math.PaddedBigBytes(key.D, 32))

	if err != nil {
		panic(err)
	}

	return sig, prefixed
}

func random1Polynomial(size int) []fr.Element {
	f := make([]fr.Element, size)
	for i := 0; i < size; i++ {
		f[i].SetRandom()
	}
	return f
}

/*
func main() {
	fmt.Println("The steps to generate CD(commit data)")
	//sdk := NewDomiconSdk(dSrsSize)
	fmt.Println("1. load SRS file to init domicon SDK")
	sdk, err := InitDomiconSdk(dSrsSize, "./srs")
	if err != nil {
		fmt.Println("InitDomiconSdk failed")
		return
	}

	fmt.Println("2. prepare test data ")
	data := make([]byte, dChunkSize*17)
	for i := range data {
		data[i] = 1
	}

	fmt.Print("3. generate data commit")
	digest, err := sdk.GenerateDataCommit(data)
	if err != nil {
		fmt.Println("GenerateDataCommit failed")
		return
	}
	fmt.Println("commit data is:", digest.Bytes())
}
*/
