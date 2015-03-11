package gazette

import (
	"bytes"
	"io"
)

type RecordedCreateCall struct {
	Bucket, Name, ContentType, ContentEncoding string
	MD5Sum                                     []byte
}
type MockStorageContext struct {
	// Recorded arguments to |Create()| call.
	RecordedCreate      []RecordedCreateCall
	RecordedErrOccurred []error
	RecordedWrites      bytes.Buffer

	CreateReturn error // Returned on next call to Create().
	WriteReturn  error // Returned on next call to Write().
	CloseReturn  error // Returned on next call to Close().
}

func (s *MockStorageContext) Create(name, contentType, encoding string,
	md5Sum []byte) (io.WriteCloser, error) {

	s.RecordedCreate = append(s.RecordedCreate,
		RecordedCreateCall{
			Name:            name,
			ContentType:     contentType,
			ContentEncoding: encoding,
			MD5Sum:          md5Sum,
		})
	return s, s.CreateReturn
}

func (s *MockStorageContext) Write(p []byte) (int, error) {
	s.RecordedWrites.Write(p)
	return len(p), s.WriteReturn
}
func (s *MockStorageContext) Close() error {
	return s.CloseReturn
}
func (s *MockStorageContext) ErrorOccurred(err error) error {
	s.RecordedErrOccurred = append(s.RecordedErrOccurred, err)
	return err
}
