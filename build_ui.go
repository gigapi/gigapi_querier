//go:build ignore
// +build ignore

package main

import (
	"archive/zip"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
)

const (
	defaultUIReleaseURL = "https://github.com/gigapi/gigapi-ui/releases/download/v1.0.0/release.zip"
	uiZipFile           = "ui.zip"
	targetDir           = "querier"
)

func main() {
	uiReleaseURL := getenv("UI_RELEASE_URL", defaultUIReleaseURL)

	// Download the UI zip file
	fmt.Println("Downloading UI release from:", uiReleaseURL)
	if err := downloadFile(uiReleaseURL, uiZipFile); err != nil {
		exitf("Error downloading UI: %v", err)
	}

	// Extract the zip file to querier/ (will create querier/dist)
	fmt.Println("Extracting UI files to querier/ ...")
	if err := extractZip(uiZipFile, targetDir); err != nil {
		exitf("Error extracting UI: %v", err)
	}

	// Clean up the zip file
	os.Remove(uiZipFile)
	fmt.Println("UI successfully downloaded and extracted to querier/dist")
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func exitf(format string, a ...interface{}) {
	fmt.Printf(format+"\n", a...)
	os.Exit(1)
}

func downloadFile(url, filepath string) error {
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("bad status: %s", resp.Status)
	}
	out, err := os.Create(filepath)
	if err != nil {
		return err
	}
	defer out.Close()
	_, err = io.Copy(out, resp.Body)
	return err
}

func extractZip(zipFile, destDir string) error {
	r, err := zip.OpenReader(zipFile)
	if err != nil {
		return err
	}
	defer r.Close()
	for _, f := range r.File {
		fpath := filepath.Join(destDir, f.Name)
		if f.FileInfo().IsDir() {
			os.MkdirAll(fpath, 0755)
			continue
		}
		if err = os.MkdirAll(filepath.Dir(fpath), 0755); err != nil {
			return err
		}
		outFile, err := os.OpenFile(fpath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
		if err != nil {
			return err
		}
		rc, err := f.Open()
		if err != nil {
			outFile.Close()
			return err
		}
		_, err = io.Copy(outFile, rc)
		outFile.Close()
		rc.Close()
		if err != nil {
			return err
		}
	}
	return nil
} 