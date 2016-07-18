<?php
/**
 * @author Yaron Koren
 * @ingroup Cargo
 */

class CargoGalleryFormat extends CargoDisplayFormat {

	function allowedParameters() {
		return array( 'mode','show bytes','show filename','per row','image width','image height' );
	}

	function getFileTitles( $valuesTable, $fieldDescriptions, $captionField, $altField, $linkField ) {		
		$fileField = null;
		foreach ( $fieldDescriptions as $field => $fieldDesc ) {
			if ( $fieldDesc->mType == 'File' ) {
				$fileField = $field;
				break;
			}
		}

		// If there's no 'File' field in the schema, just use the
		// page name.
		if ( $fileField == null ) {
			$usingPageName = true;
			$fileField = '_pageName';
		} else {
			$usingPageName = false;
		}

		$fileNames = array();
		foreach ( $valuesTable as $row ) {
			if ( array_key_exists( $fileField, $row ) ) {
				$caption = ( $captionField == null ) ? null : $row[$captionField];
				$alt = ( $altField == null ) ? null : $row[$altField];
				$link = ( $linkField == null ) ? null : Title::newFromText( $row[$linkField] );
				$fileNames[] = array(
					'title' => $row[$fileField],
					'caption' => $caption,
					'alt' => $alt,
					'link' => $link
				);				
			}
		}

		$files = array();
		foreach( $fileNames as $f ) {
			if ( $usingPageName ) {
				$title = Title::newFromText( $f['title'] );
				if ( $title == null || $title->getNamespace() != NS_FILE ) {
					continue;
				}
			} else {
				$title = Title::makeTitleSafe( NS_FILE, $f['title'] );
				if ( $title == null ) {
					continue;
				}
			}
			
			$files[] = array(
				'title' => $title,
				'caption' => CargoUtils::smartParse( $f['caption'], null ),
				'alt' => $f['alt'],
				'link' => ( $f['link'] !== null ) ? $f['link']->getLinkURL() : null
			);

		}

		return $files;
	}

	/**
	 *
	 * @param array $valuesTable Unused
	 * @param array $formattedValuesTable
	 * @param array $fieldDescriptions
	 * @param array $displayParams Unused
	 * @return string HTML
	 */
	function display( $valuesTable, $formattedValuesTable, $fieldDescriptions, $displayParams ) {
		$this->mOutput->addModules( 'mediawiki.page.gallery' );
		$this->mOutput->addModuleStyles( 'mediawiki.page.gallery.styles' );

		if ( array_key_exists( 'caption field', $displayParams ) ) {
			$captionField = str_replace( '_', ' ', $displayParams['caption field'] );
			if ( $captionField[0] == ' ' ) {
				$captionField[0] = '_';
			}
			if ( count( $valuesTable ) > 0 && !array_key_exists( $captionField, $valuesTable[0] ) ) {
				throw new MWException( "Error: the caption field \"$captionField\" must be among this query's fields." );
			}
			$this->undisplayedFields[] = $captionField;
		} else {
			$captionField = null;
		}
		if ( array_key_exists( 'alt field', $displayParams ) ) {
			$altField = str_replace( '_', ' ', $displayParams['alt field'] );
			if ( $altField[0] == ' ' ) {
				$altField[0] = '_';
			}
			if ( count( $valuesTable ) > 0 && !array_key_exists( $altField, $valuesTable[0] ) ) {
				throw new MWException( "Error: the alt field \"$altField\" must be among this query's fields." );
			}
			$this->undisplayedFields[] = $altField;
		} else {
			$altField = null;
		}
		if ( array_key_exists( 'link field', $displayParams ) ) {
			$linkField = str_replace( '_', ' ', $displayParams['link field'] );
			if ( $linkField[0] == ' ' ) {
				$linkField[0] = '_';
			}
			if ( count( $valuesTable ) > 0 && !array_key_exists( $linkField, $valuesTable[0] ) ) {
				throw new MWException( "Error: the link field \"$linkField\" must be among this query's fields." );
			}
			$this->undisplayedFields[] = $linkField;
		} else {
			$linkField = null;
		}
		
		$files = self::getFileTitles( $valuesTable, $fieldDescriptions, $captionField, $altField, $linkField );
		// Display mode - can be 'traditional'/null, 'nolines',
		// 'packed', 'packed-overlay' or 'packed-hover'; see
		// https://www.mediawiki.org/wiki/Help:Images#Mode_parameter
		$mode = ( array_key_exists( 'mode', $displayParams ) ) ?
			$displayParams['mode'] : null;

		try {
			// @TODO - it would be nice to pass in a context here,
			// if that's possible.
			$gallery = ImageGalleryBase::factory( $mode );
		} catch ( MWException $e ) {
			// User specified something invalid, fallback to default.
			$gallery = ImageGalleryBase::factory( false );
		}
		if ( array_key_exists( 'show bytes', $displayParams ) ) {
			$gallery->setShowBytes( $displayParams['show bytes'] );
		}
		if ( array_key_exists( 'show filename', $displayParams ) ) {
			$gallery->setShowFilename( $displayParams['show filename'] );
		}
		if ( array_key_exists( 'per row', $displayParams ) ) {
			$gallery->setPerRow( $displayParams['per row'] );
		}
		if ( array_key_exists( 'image width', $displayParams ) ) {
			$gallery->setWidths( $displayParams['image width'] );
		}
		if ( array_key_exists( 'image height', $displayParams ) ) {
			$gallery->setHeights( $displayParams['image height']);
		}

		foreach ( $files as $file ) {
			$gallery->add( $file['title'], $file['caption'], $file['alt'], $file['link'] );
		}

		$text = "<div id=\"mw-category-media\">\n";
		$text .= $gallery->toHTML();
		$text .= "\n</div>";

		return $text;
	}

}
