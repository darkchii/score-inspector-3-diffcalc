const { DataTypes } = require("sequelize");

const AltScoreAttributeModel = (db) => db.define('ScoreAttribute', {
    score_id: { type: DataTypes.BIGINT, primaryKey: true },
    attr_diff: { type: DataTypes.JSON },
    attr_date: { type: DataTypes.DATE },
    attr_recalc: { type: DataTypes.BOOLEAN },
}, {
    tableName: 'scoreattribute',
    timestamps: false
});

module.exports = AltScoreAttributeModel;