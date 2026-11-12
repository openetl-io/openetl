# README artwork

`readme-header.svg` is the self-contained vector banner embedded in the repository README. Its lettering is outlined so it renders without installed fonts or network requests, including when GitHub displays it as an image.

Edit `readme-header.source.svg` in a vector editor. It keeps the lettering as text and uses [Barlow Condensed SemiBold](https://fonts.google.com/specimen/Barlow+Condensed) and [Manrope](https://fonts.google.com/specimen/Manrope), matching the main website. Both fonts are available under the SIL Open Font License. Install the fonts before editing, then convert the text to paths and save the export as `readme-header.svg`. Preserve the SVG title, description, IDs, and viewBox. The source and export should have the same appearance.

The mark reuses the geometry in `web/public/logo.svg`. The palette follows the main website: warm paper, dark ink, and orange streams. The illustration represents data movement; its shapes and spacing encode no benchmark measurements. Keep performance messaging qualitative and leave out unpublished results and competitor rankings.
